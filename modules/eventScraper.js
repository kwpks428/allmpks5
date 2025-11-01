const { ethers } = require('ethers');
const moment = require('moment-timezone');

/**
 * 事件抓取器 - 优化版本
 * 严格按照：当前局次开始时间 -> 下一局开始时间 的区块范围策略
 * 最小化RPC调用，精确区块范围定位
 */
class EventScraper {
    constructor(rpcUrl, contractAddress, abi) {
        this.provider = new ethers.JsonRpcProvider(rpcUrl);
        this.contractAddress = contractAddress;
        this.contract = new ethers.Contract(contractAddress, abi, this.provider);
        
        // 事件過濾器
        this.filters = {
            startRound: this.contract.filters.StartRound(),
            lockRound: this.contract.filters.LockRound(),
            endRound: this.contract.filters.EndRound(),
            betBull: this.contract.filters.BetBull(),
            betBear: this.contract.filters.BetBear(),
            claim: this.contract.filters.Claim()
        };
        
        this.weiToBNB = ethers.parseEther('1');
        
        // RPC调用统计
        this.rpcCallCount = 0;
        this.lastResetTime = Date.now();
    }

    /**
     * 重置RPC调用统计
     */
    resetRpcStats() {
        this.rpcCallCount = 0;
        this.lastResetTime = Date.now();
    }

    /**
     * 记录RPC调用
     */
    trackRpcCall() {
        this.rpcCallCount++;
    }

    /**
     * 获取RPC调用统计
     */
    getRpcStats() {
        const elapsed = (Date.now() - this.lastResetTime) / 1000;
        return {
            totalCalls: this.rpcCallCount,
            elapsedSeconds: elapsed,
            callsPerSecond: elapsed > 0 ? (this.rpcCallCount / elapsed).toFixed(2) : 0
        };
    }

    /**
     * 獲取當前最新局次
     * @returns {Promise<number>} 當前局次
     */
    async getCurrentEpoch() {
        try {
            this.trackRpcCall();
            const currentEpoch = await this.contract.currentEpoch();
            console.log(`📊 当前最新局次: ${Number(currentEpoch)}`);
            return Number(currentEpoch);
        } catch (error) {
            console.error('❌ 獲取當前局次失敗:', error);
            throw error;
        }
    }

    /**
     * 🎯 核心方法：严格按照时间范围获取区块范围
     * 策略：当前局次开始时间 -> 下一局开始时间
     * @param {number} epoch 局次編號
     * @returns {Promise<Object>} 區塊範圍 {from, to, timeRange}
     */
    async getBlockRangeForEpoch(epoch) {
        try {
            console.log(`🔍 为局次 ${epoch} 搜索精确区块范围...`);
            
            // 1. 获取当前局次的时间戳信息
            this.trackRpcCall();
            const currentRoundInfo = await this.contract.rounds(epoch);
            const startTime = Number(currentRoundInfo.startTimestamp);
            
            if (startTime === 0) {
                throw new Error(`局次 ${epoch} 尚未开始或无效`);
            }

            console.log(`⏰ 局次 ${epoch} 开始时间: ${new Date(startTime * 1000).toISOString()}`);

            // 2. 获取下一局的开始时间作为结束边界
            let endTime;
            let nextEpochExists = false;
            
            try {
                this.trackRpcCall();
                const nextRoundInfo = await this.contract.rounds(epoch + 1);
                const nextStartTime = Number(nextRoundInfo.startTimestamp);
                
                if (nextStartTime > 0) {
                    endTime = nextStartTime;
                    nextEpochExists = true;
                    console.log(`⏰ 局次 ${epoch + 1} 开始时间: ${new Date(endTime * 1000).toISOString()}`);
                } else {
                    // 下一局还没开始，使用当前时间
                    endTime = Math.floor(Date.now() / 1000);
                    console.log(`⚠️ 局次 ${epoch + 1} 尚未开始，使用当前时间作为结束时间`);
                }
            } catch (error) {
                // 下一局不存在，使用当前时间
                endTime = Math.floor(Date.now() / 1000);
                console.log(`⚠️ 无法获取局次 ${epoch + 1}，使用当前时间作为结束时间`);
            }

            // 3. 时间范围验证
            if (endTime <= startTime) {
                throw new Error(`时间范围无效: 结束时间(${endTime}) <= 开始时间(${startTime})`);
            }

            const duration = endTime - startTime;
            console.log(`⏱️ 时间范围: ${duration} 秒 (${Math.floor(duration / 60)} 分钟)`);

            // 4. 使用精确的二分搜索找到区块范围
            console.log(`🎯 开始精确的区块搜索...`);
            
            const startBlock = await this.findExactBlockByTimestamp(startTime, 'start');
            const endBlock = await this.findExactBlockByTimestamp(endTime, 'end');

            // 5. 结果验证
            if (endBlock < startBlock) {
                throw new Error(`区块范围错误: 结束区块(${endBlock}) < 开始区块(${startBlock})`);
            }

            const blockCount = endBlock - startBlock + 1;
            const stats = this.getRpcStats();
            
            console.log(`✅ 局次 ${epoch} 区块范围确定:`);
            console.log(`   📍 起始区块: ${startBlock}`);
            console.log(`   📍 结束区块: ${endBlock}`);
            console.log(`   📊 区块总数: ${blockCount.toLocaleString()}`);
            console.log(`   🚀 RPC调用: ${stats.totalCalls} 次 (${stats.callsPerSecond}/秒)`);
            
            return {
                from: startBlock,
                to: endBlock,
                timeRange: {
                    startTime,
                    endTime,
                    duration,
                    nextEpochExists
                },
                stats: {
                    blockCount,
                    rpcCalls: stats.totalCalls
                }
            };

        } catch (error) {
            console.error(`❌ 为局次 ${epoch} 搜索区块范围失败:`, error);
            throw error;
        }
    }

    /**
     * 🎯 精确的时间戳到区块号转换
     * @param {number} targetTime 目标时间戳
     * @param {string} type 搜索类型: 'start' | 'end'
     * @returns {Promise<number>} 区块号
     */
    async findExactBlockByTimestamp(targetTime, type = 'start') {
        const isStartSearch = type === 'start';
        const searchDesc = isStartSearch ? '第一个 >= 目标时间' : '最后一个 < 目标时间';
        
        console.log(`🔍 二分搜索: 寻找${searchDesc}的区块 (目标: ${new Date(targetTime * 1000).toISOString()})`);

        this.trackRpcCall();
        const latestBlock = await this.provider.getBlockNumber();
        
        let left = 0;
        let right = latestBlock;
        let result = isStartSearch ? latestBlock : 0;
        let iterations = 0;
        const maxIterations = Math.ceil(Math.log2(latestBlock)) + 5; // 理论最大迭代次数

        while (left <= right && iterations < maxIterations) {
            iterations++;
            const mid = Math.floor((left + right) / 2);
            
            try {
                this.trackRpcCall();
                const block = await this.provider.getBlock(mid);
                const blockTime = block.timestamp;
                
                // 进度日志 (每1000次迭代或接近完成时)
                if (iterations % 10 === 0 || right - left < 1000) {
                    console.log(`   📊 迭代 ${iterations}: 区块 ${mid}, 时间差 ${blockTime - targetTime}s`);
                }

                if (isStartSearch) {
                    // 寻找第一个 >= targetTime 的区块
                    if (blockTime >= targetTime) {
                        result = mid;
                        right = mid - 1;  // 继续向左寻找更早的符合条件的区块
                    } else {
                        left = mid + 1;   // 向右寻找
                    }
                } else {
                    // 寻找最后一个 < targetTime 的区块  
                    if (blockTime < targetTime) {
                        result = mid;
                        left = mid + 1;   // 继续向右寻找更晚的符合条件的区块
                    } else {
                        right = mid - 1;  // 向左寻找
                    }
                }

            } catch (error) {
                console.warn(`   ⚠️ 获取区块 ${mid} 失败: ${error.message}`);
                right = mid - 1; // 向左调整搜索范围
            }
        }

        // 验证结果
        try {
            this.trackRpcCall();
            const resultBlock = await this.provider.getBlock(result);
            const timeDiff = resultBlock.timestamp - targetTime;
            
            console.log(`   ✅ 搜索完成: 区块 ${result}, 时间差 ${timeDiff}s, 迭代 ${iterations} 次`);
            
            // 结果合理性检查
            if (isStartSearch && timeDiff < -300) { // 开始区块不应该比目标时间早太多
                console.warn(`   ⚠️ 警告: 开始区块时间比目标时间早 ${-timeDiff} 秒`);
            } else if (!isStartSearch && timeDiff > 300) { // 结束区块不应该比目标时间晚太多
                console.warn(`   ⚠️ 警告: 结束区块时间比目标时间晚 ${timeDiff} 秒`);
            }
            
        } catch (error) {
            console.warn(`   ⚠️ 无法验证结果区块 ${result}: ${error.message}`);
        }

        return result;
    }

    /**
     * 批量抓取指定區塊範圍內的所有事件
     * 优化：智能分批，避免RPC限制
     * @param {number} fromBlock 起始區塊
     * @param {number} toBlock 結束區塊
     * @returns {Promise<Object>} 事件數據
     */
    async fetchEventsInRange(fromBlock, toBlock) {
        try {
            const blockCount = toBlock - fromBlock + 1;
            console.log(`📊 开始抓取区块范围 ${fromBlock.toLocaleString()} - ${toBlock.toLocaleString()} (${blockCount.toLocaleString()} 个区块)`);
            
            const events = {
                startRoundEvents: [],
                lockRoundEvents: [],
                endRoundEvents: [],
                betBullEvents: [],
                betBearEvents: [],
                claimEvents: [],
                totalEvents: 0
            };

            // 智能分批策略
            const maxBlocksPerBatch = 50000; // 保守值，避免RPC限制
            const totalBatches = Math.ceil(blockCount / maxBlocksPerBatch);

            console.log(`📦 将分 ${totalBatches} 个批次处理，每批最多 ${maxBlocksPerB
