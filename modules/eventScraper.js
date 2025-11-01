const { ethers } = require('ethers');
const moment = require('moment-timezone');

/**
<<<<<<< HEAD
 * 事件抓取器 - 优化版本
 * 严格按照：当前局次开始时间 -> 下一局开始时间 的区块范围策略
 * 最小化RPC调用，精确区块范围定位
=======
 * 事件抓取器 - 修复版本
 * 严格按照：当前局次开始时间 -> 下一局开始时间 的区块范围策略
 * 修复了时间戳获取问题
>>>>>>> dce8e2f (修復數據庫約束違反問題和字段匹配問題)
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
<<<<<<< HEAD
        
=======

>>>>>>> dce8e2f (修復數據庫約束違反問題和字段匹配問題)
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
<<<<<<< HEAD
     * @param {number} epoch 局次編號
     * @returns {Promise<Object>} 區塊範圍 {from, to, timeRange}
     */
    async getBlockRangeForEpoch(epoch) {
        try {
            console.log(`🔍 为局次 ${epoch} 搜索精确区块范围...`);
            
=======
     */
    async getBlockRangeForEpoch(epoch) {
        try {
            console.log(`🔍 为局次 ${epoch} 获取区块范围...`);

>>>>>>> dce8e2f (修復數據庫約束違反問題和字段匹配問題)
            // 1. 获取当前局次的时间戳信息
            this.trackRpcCall();
            const currentRoundInfo = await this.contract.rounds(epoch);
            const startTime = Number(currentRoundInfo.startTimestamp);
<<<<<<< HEAD
            
=======

>>>>>>> dce8e2f (修復數據庫約束違反問題和字段匹配問題)
            if (startTime === 0) {
                throw new Error(`局次 ${epoch} 尚未开始或无效`);
            }

            console.log(`⏰ 局次 ${epoch} 开始时间: ${new Date(startTime * 1000).toISOString()}`);

            // 2. 获取下一局的开始时间作为结束边界
            let endTime;
<<<<<<< HEAD
            let nextEpochExists = false;
            
=======
>>>>>>> dce8e2f (修復數據庫約束違反問題和字段匹配問題)
            try {
                this.trackRpcCall();
                const nextRoundInfo = await this.contract.rounds(epoch + 1);
                const nextStartTime = Number(nextRoundInfo.startTimestamp);
<<<<<<< HEAD
                
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
=======

                if (nextStartTime > 0) {
                    endTime = nextStartTime;
                    console.log(`⏰ 局次 ${epoch + 1} 开始时间: ${new Date(endTime * 1000).toISOString()}`);
                } else {
                    endTime = Math.floor(Date.now() / 1000);
                    console.log(`⚠️ 局次 ${epoch + 1} 尚未开始，使用当前时间`);
                }
            } catch (error) {
                endTime = Math.floor(Date.now() / 1000);
                console.log(`⚠️ 无法获取局次 ${epoch + 1}，使用当前时间`);
            }

            const duration = endTime - startTime;
            console.log(`⏱️ 时间范围: ${duration} 秒 (${Math.floor(duration / 60)} 分钟)`);

            // 3. 使用二分搜索找到区块范围
            const startBlock = await this.findBlockByTimestamp(startTime);
            const endBlock = await this.findBlockByTimestamp(endTime) - 1; // 不包含下一局的第一个区块

            console.log(`✅ 局次 ${epoch} 区块范围确定:`);
            console.log(`   📍 起始区块: ${startBlock}`);
            console.log(`   📍 结束区块: ${endBlock}`);
            console.log(`   📊 区块总数: ${(endBlock - startBlock + 1).toLocaleString()}`);

            return { from: startBlock, to: endBlock };

        } catch (error) {
            console.error(`❌ 为局次 ${epoch} 获取区块范围失败:`, error);
>>>>>>> dce8e2f (修復數據庫約束違反問題和字段匹配問題)
            throw error;
        }
    }

    /**
<<<<<<< HEAD
     * 🎯 精确的时间戳到区块号转换
     * @param {number} targetTime 目标时间戳
     * @param {string} type 搜索类型: 'start' | 'end'
     * @returns {Promise<number>} 区块号
     */
    async findExactBlockByTimestamp(targetTime, type = 'start') {
        const isStartSearch = type === 'start';
        const searchDesc = isStartSearch ? '第一个 >= 目标时间' : '最后一个 < 目标时间';
        
        console.log(`🔍 二分搜索: 寻找${searchDesc}的区块 (目标: ${new Date(targetTime * 1000).toISOString()})`);
=======
     * 二分搜索找到时间戳对应的区块
     */
    async findBlockByTimestamp(targetTime) {
        this.trackRpcCall();
        const latestBlock = await this.provider.getBlockNumber();
        let left = 0;
        let right = latestBlock;
        let result = latestBlock;
>>>>>>> dce8e2f (修復數據庫約束違反問題和字段匹配問題)

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
<<<<<<< HEAD
                
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
=======

                if (blockTime >= targetTime) {
                    result = mid;
                    right = mid - 1;
                } else {
                    left = mid + 1;
                }

            } catch (error) {
                right = mid - 1;
>>>>>>> dce8e2f (修復數據庫約束違反問題和字段匹配問題)
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
<<<<<<< HEAD
     * 批量抓取指定區塊範圍內的所有事件
     * 优化：智能分批，避免RPC限制
     * @param {number} fromBlock 起始區塊
     * @param {number} toBlock 結束區塊
     * @returns {Promise<Object>} 事件數據
=======
     * 抓取指定区块范围内的所有事件
>>>>>>> dce8e2f (修復數據庫約束違反問題和字段匹配問題)
     */
    async fetchEventsInRange(fromBlock, toBlock) {
        try {
            const blockCount = toBlock - fromBlock + 1;
            console.log(`📊 开始抓取区块范围 ${fromBlock.toLocaleString()} - ${toBlock.toLocaleString()} (${blockCount.toLocaleString()} 个区块)`);
<<<<<<< HEAD
            
=======

>>>>>>> dce8e2f (修復數據庫約束違反問題和字段匹配問題)
            const events = {
                startRoundEvents: [],
                lockRoundEvents: [],
                endRoundEvents: [],
                betBullEvents: [],
                betBearEvents: [],
                claimEvents: [],
                totalEvents: 0
            };

<<<<<<< HEAD
            // 智能分批策略
            const maxBlocksPerBatch = 50000; // 保守值，避免RPC限制
            const totalBatches = Math.ceil(blockCount / maxBlocksPerBatch);

            console.log(`📦 将分 ${totalBatches} 个批次处理，每批最多 ${maxBlocksPerB
=======
            // 并行抓取所有事件类型
            const [
                startRoundEvents,
                lockRoundEvents,
                endRoundEvents,
                betBullEvents,
                betBearEvents,
                claimEvents
            ] = await Promise.all([
                this.fetchEventsByFilter('StartRound', this.filters.startRound, fromBlock, toBlock),
                this.fetchEventsByFilter('LockRound', this.filters.lockRound, fromBlock, toBlock),
                this.fetchEventsByFilter('EndRound', this.filters.endRound, fromBlock, toBlock),
                this.fetchEventsByFilter('BetBull', this.filters.betBull, fromBlock, toBlock),
                this.fetchEventsByFilter('BetBear', this.filters.betBear, fromBlock, toBlock),
                this.fetchEventsByFilter('Claim', this.filters.claim, fromBlock, toBlock)
            ]);

            events.startRoundEvents = startRoundEvents;
            events.lockRoundEvents = lockRoundEvents;
            events.endRoundEvents = endRoundEvents;
            events.betBullEvents = betBullEvents;
            events.betBearEvents = betBearEvents;
            events.claimEvents = claimEvents;

            events.totalEvents = startRoundEvents.length + lockRoundEvents.length +
                endRoundEvents.length + betBullEvents.length +
                betBearEvents.length + claimEvents.length;

            console.log(`✅ 事件抓取完成:`);
            console.log(`   🎯 StartRound: ${events.startRoundEvents.length}`);
            console.log(`   🔒 LockRound: ${events.lockRoundEvents.length}`);
            console.log(`   🏁 EndRound: ${events.endRoundEvents.length}`);
            console.log(`   🐂 BetBull: ${events.betBullEvents.length.toLocaleString()}`);
            console.log(`   🐻 BetBear: ${events.betBearEvents.length.toLocaleString()}`);
            console.log(`   💰 Claim: ${events.claimEvents.length.toLocaleString()}`);
            console.log(`   📊 总计: ${events.totalEvents.toLocaleString()} 个事件`);

            return events;

        } catch (error) {
            console.error('❌ 抓取事件失败:', error);
            throw error;
        }
    }

    /**
     * 🎯 修复版：按事件类型抓取 - 现在会获取真实时间戳
     */
    async fetchEventsByFilter(eventName, filter, fromBlock, toBlock) {
        try {
            this.trackRpcCall();
            const rawEvents = await this.contract.queryFilter(filter, fromBlock, toBlock);
            return await this.parseEvents(rawEvents, eventName); // 🎯 改为 await
        } catch (error) {
            console.warn(`⚠️ 抓取 ${eventName} 事件失败 (区块 ${fromBlock}-${toBlock}):`, error.message);
            return [];
        }
    }

    /**
     * 🎯 修复版：解析原始事件数据并获取真实时间戳
     * @param {Array} rawEvents 原始事件数组
     * @param {string} eventType 事件类型
     * @returns {Promise<Array>} 解析后的事件数组
     */
    async parseEvents(rawEvents, eventType) {
        if (!rawEvents || rawEvents.length === 0) {
            return [];
        }

        const parsedEvents = [];

        // 🎯 为了优化性能，批量获取区块时间戳
        const blockNumbers = [...new Set(rawEvents.map(event => event.blockNumber))];
        const blockTimestamps = new Map();

        console.log(`   📅 获取 ${blockNumbers.length} 个区块的时间戳 (${eventType})...`);

        // 批量获取区块时间戳
        for (const blockNumber of blockNumbers) {
            try {
                this.trackRpcCall();
                const block = await this.provider.getBlock(blockNumber);
                blockTimestamps.set(blockNumber, block.timestamp);
            } catch (error) {
                console.warn(`   ⚠️ 获取区块 ${blockNumber} 时间戳失败: ${error.message}`);
                blockTimestamps.set(blockNumber, Math.floor(Date.now() / 1000)); // 使用当前时间作为备用
            }
        }

        // 解析每个事件
        for (const event of rawEvents) {
            const timestamp = blockTimestamps.get(event.blockNumber) || Math.floor(Date.now() / 1000);

            const baseEvent = {
                eventType,
                blockNumber: event.blockNumber,
                blockHash: event.blockHash,
                transactionHash: event.transactionHash,
                transactionIndex: event.transactionIndex,
                logIndex: event.logIndex,
                address: event.address,
                timestamp: timestamp // 🎯 添加真实时间戳
            };

            try {
                switch (eventType) {
                    case 'StartRound':
                        parsedEvents.push({
                            ...baseEvent,
                            epoch: Number(event.args.epoch)
                        });
                        break;

                    case 'LockRound':
                        parsedEvents.push({
                            ...baseEvent,
                            epoch: Number(event.args.epoch),
                            oracleId: event.args.oracleId,
                            price: Number(ethers.formatEther(event.args.price))
                        });
                        break;

                    case 'EndRound':
                        parsedEvents.push({
                            ...baseEvent,
                            epoch: Number(event.args.epoch),
                            oracleId: event.args.oracleId,
                            price: Number(ethers.formatEther(event.args.price))
                        });
                        break;

                    case 'BetBull':
                    case 'BetBear':
                        parsedEvents.push({
                            ...baseEvent,
                            sender: event.args.sender,
                            epoch: Number(event.args.epoch),
                            amount: Number(ethers.formatEther(event.args.amount)),
                            position: eventType === 'BetBull' ? 'Bull' : 'Bear'
                        });
                        break;

                    case 'Claim':
                        parsedEvents.push({
                            ...baseEvent,
                            sender: event.args.sender,
                            epoch: Number(event.args.epoch),
                            amount: Number(ethers.formatEther(event.args.amount))
                        });
                        break;

                    default:
                        console.warn(`⚠️ 未知事件类型: ${eventType}`);
                        parsedEvents.push(baseEvent);
                }
            } catch (parseError) {
                console.warn(`⚠️ 解析 ${eventType} 事件失败:`, parseError);
                parsedEvents.push(baseEvent);
            }
        }

        return parsedEvents;
    }

    /**
     * 获取指定局次的完整事件数据
     */
    async getEventsForEpoch(epoch) {
        try {
            console.log(`🎯 开始获取局次 ${epoch} 的事件数据...`);
            this.resetRpcStats();

            // 1. 获取区块范围（当局开始时间 -> 下一局开始时间）
            const blockRange = await this.getBlockRangeForEpoch(epoch);

            // 2. 抓取所有事件
            const events = await this.fetchEventsInRange(blockRange.from, blockRange.to);

            // 3. 过滤确保只返回指定局次的事件
            const filteredEvents = {
                startRoundEvents: events.startRoundEvents.filter(e => e.epoch === epoch),
                lockRoundEvents: events.lockRoundEvents.filter(e => e.epoch === epoch),
                endRoundEvents: events.endRoundEvents.filter(e => e.epoch === epoch),
                betBullEvents: events.betBullEvents.filter(e => e.epoch === epoch),
                betBearEvents: events.betBearEvents.filter(e => e.epoch === epoch),
                claimEvents: events.claimEvents.filter(e => e.epoch === epoch),
                blockRange,
                totalEvents: 0
            };

            filteredEvents.totalEvents = filteredEvents.startRoundEvents.length +
                filteredEvents.lockRoundEvents.length +
                filteredEvents.endRoundEvents.length +
                filteredEvents.betBullEvents.length +
                filteredEvents.betBearEvents.length +
                filteredEvents.claimEvents.length;

            console.log(`✅ 局次 ${epoch} 事件数据获取完成: ${filteredEvents.totalEvents.toLocaleString()} 个事件`);

            return filteredEvents;

        } catch (error) {
            console.error(`❌ 获取局次 ${epoch} 事件数据失败:`, error);
            throw error;
        }
    }

    /**
     * 获取局次的基本信息
     */
    async getRoundInfo(epoch) {
        try {
            this.trackRpcCall();
            const roundInfo = await this.contract.rounds(epoch);

            return {
                epoch: Number(epoch),
                startTimestamp: Number(roundInfo.startTimestamp),
                lockTimestamp: Number(roundInfo.lockTimestamp),
                closeTimestamp: Number(roundInfo.closeTimestamp),
                lockPrice: roundInfo.lockPrice ? Number(ethers.formatEther(roundInfo.lockPrice)) : 0,
                closePrice: roundInfo.closePrice ? Number(ethers.formatEther(roundInfo.closePrice)) : 0,
                lockOracleId: roundInfo.lockOracleId || '0',
                closeOracleId: roundInfo.closeOracleId || '0',
                totalAmount: roundInfo.totalAmount ? Number(ethers.formatEther(roundInfo.totalAmount)) : 0,
                bullAmount: roundInfo.bullAmount ? Number(ethers.formatEther(roundInfo.bullAmount)) : 0,
                bearAmount: roundInfo.bearAmount ? Number(ethers.formatEther(roundInfo.bearAmount)) : 0,
                rewardBaseCalAmount: roundInfo.rewardBaseCalAmount ? Number(ethers.formatEther(roundInfo.rewardBaseCalAmount)) : 0,
                rewardAmount: roundInfo.rewardAmount ? Number(ethers.formatEther(roundInfo.rewardAmount)) : 0,
                oracleCalled: roundInfo.oracleCalled || false
            };
        } catch (error) {
            console.error(`❌ 获取局次 ${epoch} 基本信息失败:`, error);
            throw error;
        }
    }

    /**
     * 检查当前是否可以处理指定局次
     */
    async canProcessEpoch(epoch) {
        try {
            const roundInfo = await this.getRoundInfo(epoch);
            const currentTime = Math.floor(Date.now() / 1000);

            // 检查局次是否已经结束（有closeTimestamp且不为0）
            if (roundInfo.closeTimestamp === 0) {
                console.log(`⚠️ 局次 ${epoch} 尚未结束 (closeTimestamp = 0)`);
                return false;
            }

            // 检查是否已经调用了oracle（确保数据完整）
            if (!roundInfo.oracleCalled) {
                console.log(`⚠️ 局次 ${epoch} Oracle尚未调用`);
                return false;
            }

            // 建议等待一定时间后再处理，确保所有相关事件都已上链
            const waitTime = 300; // 5分钟
            if (currentTime - roundInfo.closeTimestamp < waitTime) {
                console.log(`⚠️ 局次 ${epoch} 结束时间过近，建议等待 ${waitTime - (currentTime - roundInfo.closeTimestamp)} 秒后处理`);
                return false;
            }

            console.log(`✅ 局次 ${epoch} 可以处理`);
            return true;

        } catch (error) {
            console.error(`❌ 检查局次 ${epoch} 可处理性失败:`, error);
            return false;
        }
    }
}

module.exports = EventScraper;
>>>>>>> dce8e2f (修復數據庫約束違反問題和字段匹配問題)
