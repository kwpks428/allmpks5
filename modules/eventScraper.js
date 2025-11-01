const { ethers } = require('ethers');
const moment = require('moment-timezone');

/**
 * 事件抓取器 - 優化版本
 * 嚴格按照：當前局次開始時間 -> 下一局開始時間 的區塊範圍策略
 * 最小化RPC調用，精確區塊範圍定位
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
        
        // RPC調用統計
        this.rpcCallCount = 0;
        this.lastResetTime = Date.now();
    }

    /**
     * 重置RPC調用統計
     */
    resetRpcStats() {
        this.rpcCallCount = 0;
        this.lastResetTime = Date.now();
    }

    /**
     * 記錄RPC調用
     */
    trackRpcCall() {
        this.rpcCallCount++;
    }

    /**
     * 獲取RPC調用統計
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
            console.log(`📊 當前最新局次: ${Number(currentEpoch)}`);
            return Number(currentEpoch);
        } catch (error) {
            console.error('❌ 獲取當前局次失敗:', error);
            throw error;
        }
    }

    /**
     * 🎯 核心方法：嚴格按照時間範圍獲取區塊範圍
     * 策略：當前局次開始時間 -> 下一局開始時間
     * @param {number} epoch 局次編號
     * @returns {Promise<Object>} 區塊範圍 {from, to, timeRange}
     */
    async getBlockRangeForEpoch(epoch) {
        try {
            console.log(`🔍 為局次 ${epoch} 獲取區塊範圍...`);
            
            // 1. 獲取當前局次的時間戳信息
            this.trackRpcCall();
            const currentRoundInfo = await this.contract.rounds(epoch);
            const startTime = Number(currentRoundInfo.startTimestamp);
            
            if (startTime === 0) {
                throw new Error(`局次 ${epoch} 尚未開始或無效`);
            }

            console.log(`⏰ 局次 ${epoch} 開始時間: ${new Date(startTime * 1000).toISOString()}`);

            // 2. 獲取下一局的開始時間作為結束邊界
            let endTime;
            let nextEpochExists = false;
            
            try {
                this.trackRpcCall();
                const nextRoundInfo = await this.contract.rounds(epoch + 1);
                const nextStartTime = Number(nextRoundInfo.startTimestamp);
                
                if (nextStartTime > 0) {
                    endTime = nextStartTime;
                    nextEpochExists = true;
                    console.log(`⏰ 局次 ${epoch + 1} 開始時間: ${new Date(endTime * 1000).toISOString()}`);
                } else {
                    // 下一局還沒開始，使用當前時間
                    endTime = Math.floor(Date.now() / 1000);
                    console.log(`⚠️ 局次 ${epoch + 1} 尚未開始，使用當前時間作為結束時間`);
                }
            } catch (error) {
                // 下一局不存在，使用當前時間
                endTime = Math.floor(Date.now() / 1000);
                console.log(`⚠️ 無法獲取局次 ${epoch + 1}，使用當前時間作為結束時間`);
            }

            // 3. 時間範圍驗證
            if (endTime <= startTime) {
                throw new Error(`時間範圍無效: 結束時間(${endTime}) <= 開始時間(${startTime})`);
            }

            const duration = endTime - startTime;
            console.log(`⏱️ 時間範圍: ${duration} 秒 (${Math.floor(duration / 60)} 分鐘)`);

            // 4. 使用精確的二分搜索找到區塊範圍
            console.log(`🎯 開始精確的區塊搜索...`);
            
            const startBlock = await this.findExactBlockByTimestamp(startTime, 'start');
            const endBlock = await this.findExactBlockByTimestamp(endTime, 'end');

            // 5. 結果驗證
            if (endBlock < startBlock) {
                throw new Error(`區塊範圍錯誤: 結束區塊(${endBlock}) < 開始區塊(${startBlock})`);
            }

            const blockCount = endBlock - startBlock + 1;
            const stats = this.getRpcStats();
            
            console.log(`✅ 局次 ${epoch} 區塊範圍確定:`);
            console.log(`   📍 起始區塊: ${startBlock}`);
            console.log(`   📍 結束區塊: ${endBlock}`);
            console.log(`   📊 區塊總數: ${blockCount.toLocaleString()}`);
            console.log(`   🚀 RPC調用: ${stats.totalCalls} 次 (${stats.callsPerSecond}/秒)`);
            
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
            console.error(`❌ 為局次 ${epoch} 獲取區塊範圍失敗:`, error);
            throw error;
        }
    }

    /**
     * 🎯 精確的時間戳到區塊號轉換
     * @param {number} targetTime 目標時間戳
     * @param {string} type 搜索類型: 'start' | 'end'
     * @returns {Promise<number>} 區塊號
     */
    async findExactBlockByTimestamp(targetTime, type = 'start') {
        const isStartSearch = type === 'start';
        const searchDesc = isStartSearch ? '第一個 >= 目標時間' : '最後一個 < 目標時間';
        
        console.log(`🔍 二分搜索: 尋找${searchDesc}的區塊 (目標: ${new Date(targetTime * 1000).toISOString()})`);

        this.trackRpcCall();
        const latestBlock = await this.provider.getBlockNumber();
        
        let left = 0;
        let right = latestBlock;
        let result = isStartSearch ? latestBlock : 0;
        let iterations = 0;
        const maxIterations = Math.ceil(Math.log2(latestBlock)) + 5; // 理論最大迭代次數

        while (left <= right && iterations < maxIterations) {
            iterations++;
            const mid = Math.floor((left + right) / 2);

            try {
                this.trackRpcCall();
                const block = await this.provider.getBlock(mid);
                const blockTime = block.timestamp;
                
                // 進度日誌 (每10次迭代或接近完成時)
                if (iterations % 10 === 0 || right - left < 1000) {
                    console.log(`   📊 迭代 ${iterations}: 區塊 ${mid}, 時間差 ${blockTime - targetTime}s`);
                }

                if (isStartSearch) {
                    // 尋找第一個 >= targetTime 的區塊
                    if (blockTime >= targetTime) {
                        result = mid;
                        right = mid - 1;  // 繼續向左尋找更早的符合條件的區塊
                    } else {
                        left = mid + 1;   // 向右尋找
                    }
                } else {
                    // 尋找最後一個 < targetTime 的區塊  
                    if (blockTime < targetTime) {
                        result = mid;
                        left = mid + 1;   // 繼續向右尋找更晚的符合條件的區塊
                    } else {
                        right = mid - 1;  // 向左尋找
                    }
                }

            } catch (error) {
                console.warn(`   ⚠️ 獲取區塊 ${mid} 失敗: ${error.message}`);
                right = mid - 1; // 向左調整搜索範圍
            }
        }

        // 驗證結果
        try {
            this.trackRpcCall();
            const resultBlock = await this.provider.getBlock(result);
            const timeDiff = resultBlock.timestamp - targetTime;
            
            console.log(`   ✅ 搜索完成: 區塊 ${result}, 時間差 ${timeDiff}s, 迭代 ${iterations} 次`);
            
            // 結果合理性檢查
            if (isStartSearch && timeDiff < -300) { // 開始區塊不應該比目標時間早太多
                console.warn(`   ⚠️ 警告: 開始區塊時間比目標時間早 ${-timeDiff} 秒`);
            } else if (!isStartSearch && timeDiff > 300) { // 結束區塊不應該比目標時間晚太多
                console.warn(`   ⚠️ 警告: 結束區塊時間比目標時間晚 ${timeDiff} 秒`);
            }
            
        } catch (error) {
            console.warn(`   ⚠️ 無法驗證結果區塊 ${result}: ${error.message}`);
        }

        return result;
    }

    /**
     * 批量抓取指定區塊範圍內的所有事件
     * 優化：智能分批，避免RPC限制
     * @param {number} fromBlock 起始區塊
     * @param {number} toBlock 結束區塊
     * @returns {Promise<Object>} 事件數據
     */
    async fetchEventsInRange(fromBlock, toBlock) {
        try {
            const blockCount = toBlock - fromBlock + 1;
            console.log(`📊 開始抓取區塊範圍 ${fromBlock.toLocaleString()} - ${toBlock.toLocaleString()} (${blockCount.toLocaleString()} 個區塊)`);
            
            const events = {
                startRoundEvents: [],
                lockRoundEvents: [],
                endRoundEvents: [],
                betBullEvents: [],
                betBearEvents: [],
                claimEvents: [],
                totalEvents: 0
            };

            // 並行抓取所有事件類型
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
            console.log(`   📊 總計: ${events.totalEvents.toLocaleString()} 個事件`);

            return events;

        } catch (error) {
            console.error('❌ 抓取事件失敗:', error);
            throw error;
        }
    }

    /**
     * 🎯 修復版：按事件類型抓取 - 現在會獲取真實時間戳
     */
    async fetchEventsByFilter(eventName, filter, fromBlock, toBlock) {
        try {
            this.trackRpcCall();
            const rawEvents = await this.contract.queryFilter(filter, fromBlock, toBlock);
            return await this.parseEvents(rawEvents, eventName); // 🎯 改為 await
        } catch (error) {
            console.warn(`⚠️ 抓取 ${eventName} 事件失敗 (區塊 ${fromBlock}-${toBlock}):`, error.message);
            return [];
        }
    }

    /**
     * 🎯 修復版：解析原始事件數據並獲取真實時間戳
     * @param {Array} rawEvents 原始事件數組
     * @param {string} eventType 事件類型
     * @returns {Promise<Array>} 解析後的事件數組
     */
    async parseEvents(rawEvents, eventType) {
        if (!rawEvents || rawEvents.length === 0) {
            return [];
        }

        const parsedEvents = [];

        // 🎯 為了優化性能，批量獲取區塊時間戳
        const blockNumbers = [...new Set(rawEvents.map(event => event.blockNumber))];
        const blockTimestamps = new Map();

        console.log(`   📅 獲取 ${blockNumbers.length} 個區塊的時間戳 (${eventType})...`);

        // 批量獲取區塊時間戳
        for (const blockNumber of blockNumbers) {
            try {
                this.trackRpcCall();
                const block = await this.provider.getBlock(blockNumber);
                blockTimestamps.set(blockNumber, block.timestamp);
            } catch (error) {
                console.warn(`   ⚠️ 獲取區塊 ${blockNumber} 時間戳失敗: ${error.message}`);
                blockTimestamps.set(blockNumber, Math.floor(Date.now() / 1000)); // 使用當前時間作為備用
            }
        }

        // 解析每個事件
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
                timestamp: timestamp // 🎯 添加真實時間戳
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
                        console.warn(`⚠️ 未知事件類型: ${eventType}`);
                        parsedEvents.push(baseEvent);
                }
            } catch (parseError) {
                console.warn(`⚠️ 解析 ${eventType} 事件失敗:`, parseError);
                parsedEvents.push(baseEvent);
            }
        }

        return parsedEvents;
    }

    /**
     * 獲取指定局次的完整事件數據
     */
    async getEventsForEpoch(epoch) {
        try {
            console.log(`🎯 開始獲取局次 ${epoch} 的事件數據...`);
            this.resetRpcStats();

            // 1. 獲取區塊範圍（當局開始時間 -> 下一局開始時間）
            const blockRange = await this.getBlockRangeForEpoch(epoch);

            // 2. 抓取所有事件
            const events = await this.fetchEventsInRange(blockRange.from, blockRange.to);

            // 3. 過濾確保只返回指定局次的事件
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

            console.log(`✅ 局次 ${epoch} 事件數據獲取完成: ${filteredEvents.totalEvents.toLocaleString()} 個事件`);

            return filteredEvents;

        } catch (error) {
            console.error(`❌ 獲取局次 ${epoch} 事件數據失敗:`, error);
            throw error;
        }
    }

    /**
     * 獲取局次的基本信息
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
            console.error(`❌ 獲取局次 ${epoch} 基本信息失敗:`, error);
            throw error;
        }
    }

    /**
     * 檢查當前是否可以處理指定局次
     */
    async canProcessEpoch(epoch) {
        try {
            const roundInfo = await this.getRoundInfo(epoch);
            const currentTime = Math.floor(Date.now() / 1000);

            // 檢查局次是否已經結束（有closeTimestamp且不為0）
            if (roundInfo.closeTimestamp === 0) {
                console.log(`⚠️ 局次 ${epoch} 尚未結束 (closeTimestamp = 0)`);
                return false;
            }

            // 檢查是否已經調用了oracle（確保數據完整）
            if (!roundInfo.oracleCalled) {
                console.log(`⚠️ 局次 ${epoch} Oracle尚未調用`);
                return false;
            }

            // 建議等待一定時間後再處理，確保所有相關事件都已上鏈
            const waitTime = 300; // 5分鐘
            if (currentTime - roundInfo.closeTimestamp < waitTime) {
                console.log(`⚠️ 局次 ${epoch} 結束時間過近，建議等待 ${waitTime - (currentTime - roundInfo.closeTimestamp)} 秒後處理`);
                return false;
            }

            console.log(`✅ 局次 ${epoch} 可以處理`);
            return true;

        } catch (error) {
            console.error(`❌ 檢查局次 ${epoch} 可處理性失敗:`, error);
            return false;
        }
    }
}

module.exports = EventScraper;