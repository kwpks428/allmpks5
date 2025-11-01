const { ethers } = require('ethers');
const moment = require('moment-timezone');

/**
 * 事件抓取器
 * 負責與 BSC 區塊鏈交互，抓取合約事件並解析數據
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
    }

    /**
     * 獲取當前最新局次
     * @returns {Promise<number>} 當前局次
     */
    async getCurrentEpoch() {
        try {
            const currentEpoch = await this.contract.currentEpoch();
            return Number(currentEpoch);
        } catch (error) {
            console.error('❌ 獲取當前局次失敗:', error);
            throw error;
        }
    }

    /**
     * 使用二分搜尋法找到指定局次的區塊範圍
     * @param {number} epoch 局次編號
     * @returns {Promise<Object>} 區塊範圍 {from, to}
     */
    async getBlockRangeForEpoch(epoch) {
        try {
            console.log(`🔍 為局次 ${epoch} 搜索區塊範圍...`);
            
            // 獲取該局的基本信息
            const roundInfo = await this.contract.rounds(epoch);
            const startTime = Number(roundInfo.startTimestamp);
            const lockTime = Number(roundInfo.lockTimestamp);
            
            if (lockTime === 0) {
                throw new Error(`局次 ${epoch} 尚未開始或無效`);
            }

            // 直接使用當局開始時間到下一局開始時間的範圍
            // 這個範圍本身就包含了完整的事件流程
            let nextStartTime;
            try {
                const nextRoundInfo = await this.contract.rounds(epoch + 1);
                nextStartTime = Number(nextRoundInfo.startTimestamp);
            } catch (error) {
                // 如果獲取失敗，使用當前時間作為上限
                nextStartTime = Math.floor(Date.now() / 1000);
            }

            // 使用二分搜尋法找到對應的區塊號
            const currentBlock = await this.provider.getBlockNumber();
            const startBlock = await this.findBlockByTimestamp(startTime);
            const endBlock = await this.findBlockByTimestamp(nextStartTime - 1);
            
            console.log(`📍 局次 ${epoch} 區塊範圍: ${startBlock} - ${endBlock}`);
            console.log(`⏰ 局次時間範圍: ${new Date(startTime * 1000).toISOString()} - ${new Date(nextStartTime * 1000).toISOString()}`);
            return { from: startBlock, to: endBlock };
            
        } catch (error) {
            console.error(`❌ 為局次 ${epoch} 搜索區塊範圍失敗:`, error);
            throw error;
        }
    }

    /**
     * 二分搜尋法：根據時間戳找到對應的區塊號
     * @param {number} targetTime 目標時間戳
     * @returns {Promise<number>} 區塊號
     */
    async findBlockByTimestamp(targetTime) {
        const currentBlock = await this.provider.getBlockNumber();
        let left = 0;
        let right = currentBlock;
        let result = 0;

        while (left <= right) {
            const mid = Math.floor((left + right) / 2);
            
            try {
                const block = await this.provider.getBlock(mid);
                if (block.timestamp >= targetTime) {
                    result = mid;
                    right = mid - 1;
                } else {
                    left = mid + 1;
                }
            } catch (error) {
                console.warn(`⚠️  獲取區塊 ${mid} 信息失敗，跳過:`, error);
                right = mid - 1;
            }
        }

        return result;
    }

    /**
     * 批量抓取指定區塊範圍內的所有事件
     * @param {number} fromBlock 起始區塊
     * @param {number} toBlock 結束區塊
     * @returns {Promise<Object>} 事件數據
     */
    async fetchEventsInRange(fromBlock, toBlock) {
        try {
            console.log(`📊 抓取區塊 ${fromBlock} - ${toBlock} 的事件...`);
            
            const events = {
                startRoundEvents: [],
                lockRoundEvents: [],
                endRoundEvents: [],
                betBullEvents: [],
                betBearEvents: [],
                claimEvents: [],
                totalEvents: 0
            };

            // 分批處理（每次 10,000 個區塊）
            const batchSize = 10000;
            const batches = Math.ceil((toBlock - fromBlock + 1) / batchSize);

            for (let i = 0; i < batches; i++) {
                const batchFrom = fromBlock + (i * batchSize);
                const batchTo = Math.min(batchFrom + batchSize - 1, toBlock);
                
                console.log(`📦 處理批次 ${i + 1}/${batches}: 區塊 ${batchFrom} - ${batchTo}`);
                
                const batchEvents = await this.fetchBatchEvents(batchFrom, batchTo);
                
                // 合併結果
                events.startRoundEvents.push(...batchEvents.startRoundEvents);
                events.lockRoundEvents.push(...batchEvents.lockRoundEvents);
                events.endRoundEvents.push(...batchEvents.endRoundEvents);
                events.betBullEvents.push(...batchEvents.betBullEvents);
                events.betBearEvents.push(...batchEvents.betBearEvents);
                events.claimEvents.push(...batchEvents.claimEvents);
            }

            events.totalEvents = 
                events.startRoundEvents.length +
                events.lockRoundEvents.length +
                events.endRoundEvents.length +
                events.betBullEvents.length +
                events.betBearEvents.length +
                events.claimEvents.length;

            console.log(`✅ 總共抓取到 ${events.totalEvents} 個事件`);
            return events;
            
        } catch (error) {
            console.error('❌ 批量抓取事件失敗:', error);
            throw error;
        }
    }

    /**
     * 抓取單批事件
     * @param {number} fromBlock 起始區塊
     * @param {number} toBlock 結束區塊
     * @returns {Promise<Object>} 批次事件數據
     */
    async fetchBatchEvents(fromBlock, toBlock) {
        const events = {
            startRoundEvents: [],
            lockRoundEvents: [],
            endRoundEvents: [],
            betBullEvents: [],
            betBearEvents: [],
            claimEvents: []
        };

        try {
            // 並行抓取所有類型的事件
            const promises = [
                this.contract.queryFilter(this.filters.startRound, fromBlock, toBlock),
                this.contract.queryFilter(this.filters.lockRound, fromBlock, toBlock),
                this.contract.queryFilter(this.filters.endRound, fromBlock, toBlock),
                this.contract.queryFilter(this.filters.betBull, fromBlock, toBlock),
                this.contract.queryFilter(this.filters.betBear, fromBlock, toBlock),
                this.contract.queryFilter(this.filters.claim, fromBlock, toBlock)
            ];

            const [
                startRoundLogs,
                lockRoundLogs,
                endRoundLogs,
                betBullLogs,
                betBearLogs,
                claimLogs
            ] = await Promise.all(promises);

            // 解析事件
            events.startRoundEvents = this.parseStartRoundEvents(startRoundLogs);
            events.lockRoundEvents = this.parseLockRoundEvents(lockRoundLogs);
            events.endRoundEvents = this.parseEndRoundEvents(endRoundLogs);
            events.betBullEvents = this.parseBetBullEvents(betBullLogs);
            events.betBearEvents = this.parseBetBearEvents(betBearLogs);
            events.claimEvents = this.parseClaimEvents(claimLogs);

        } catch (error) {
            console.error(`❌ 抓取區塊 ${fromBlock}-${toBlock} 事件失敗:`, error);
            throw error;
        }

        return events;
    }

    /**
     * 解析 StartRound 事件
     * @param {Array} logs 事件日誌
     * @returns {Array} 解析後的事件數據
     */
    parseStartRoundEvents(logs) {
        return logs.map(log => {
            const parsed = this.contract.interface.parseLog(log);
            return {
                epoch: Number(parsed.args[0]),
                blockNumber: log.blockNumber,
                timestamp: Number(parsed.blockTimestamp),
                transactionHash: log.transactionHash
            };
        });
    }

    /**
     * 解析 LockRound 事件
     * @param {Array} logs 事件日誌
     * @returns {Array} 解析後的事件數據
     */
    parseLockRoundEvents(logs) {
        return logs.map(log => {
            const parsed = this.contract.interface.parseLog(log);
            return {
                epoch: Number(parsed.args[0]),
                roundId: Number(parsed.args[1]),
                price: parsed.args[2].toString(), // 保持原始字符串格式
                blockNumber: log.blockNumber,
                timestamp: Number(parsed.blockTimestamp)
            };
        });
    }

    /**
     * 解析 EndRound 事件
     * @param {Array} logs 事件日誌
     * @returns {Array} 解析後的事件數據
     */
    parseEndRoundEvents(logs) {
        return logs.map(log => {
            const parsed = this.contract.interface.parseLog(log);
            return {
                epoch: Number(parsed.args[0]),
                roundId: Number(parsed.args[1]),
                price: parsed.args[2].toString(), // 保持原始字符串格式
                blockNumber: log.blockNumber,
                timestamp: Number(parsed.blockTimestamp)
            };
        });
    }

    /**
     * 解析 BetBull 事件
     * @param {Array} logs 事件日誌
     * @returns {Array} 解析後的事件數據
     */
    parseBetBullEvents(logs) {
        return logs.map(log => {
            const parsed = this.contract.interface.parseLog(log);
            return {
                sender: parsed.args[0].toLowerCase(), // 轉為小寫
                epoch: Number(parsed.args[1]),
                amount: Number(parsed.args[2].toString()) / 1e18, // BNB 轉換
                blockNumber: log.blockNumber,
                timestamp: Number(parsed.blockTimestamp),
                transactionHash: log.transactionHash
            };
        });
    }

    /**
     * 解析 BetBear 事件
     * @param {Array} logs 事件日誌
     * @returns {Array} 解析後的事件數據
     */
    parseBetBearEvents(logs) {
        return logs.map(log => {
            const parsed = this.contract.interface.parseLog(log);
            return {
                sender: parsed.args[0].toLowerCase(), // 轉為小寫
                epoch: Number(parsed.args[1]),
                amount: Number(parsed.args[2].toString()) / 1e18, // BNB 轉換
                blockNumber: log.blockNumber,
                timestamp: Number(parsed.blockTimestamp),
                transactionHash: log.transactionHash
            };
        });
    }

    /**
     * 解析 Claim 事件
     * @param {Array} logs 事件日誌
     * @returns {Array} 解析後的事件數據
     */
    parseClaimEvents(logs) {
        return logs.map(log => {
            const parsed = this.contract.interface.parseLog(log);
            return {
                sender: parsed.args[0].toLowerCase(), // 轉為小寫
                epoch: Number(parsed.args[1]),
                amount: Number(parsed.args[2].toString()) / 1e18, // BNB 轉換
                blockNumber: log.blockNumber,
                timestamp: Number(parsed.blockTimestamp),
                transactionHash: log.transactionHash
            };
        });
    }

    /**
     * 獲取區塊的具體信息
     * @param {number} blockNumber 區塊號
     * @returns {Promise<Object>} 區塊信息
     */
    async getBlockInfo(blockNumber) {
        try {
            const block = await this.provider.getBlock(blockNumber);
            return {
                number: block.number,
                timestamp: block.timestamp,
                hash: block.hash,
                parentHash: block.parentHash
            };
        } catch (error) {
            console.error(`❌ 獲取區塊 ${blockNumber} 信息失敗:`, error);
            throw error;
        }
    }

    /**
     * 檢查區塊鏈連接狀態
     * @returns {Promise<Object>} 連接狀態
     */
    async checkConnection() {
        try {
            const blockNumber = await this.provider.getBlockNumber();
            const block = await this.provider.getBlock(blockNumber);
            
            return {
                connected: true,
                currentBlock: blockNumber,
                latestBlockTimestamp: block.timestamp,
                network: await this.provider.getNetwork()
            };
        } catch (error) {
            return {
                connected: false,
                error: error.message
            };
        }
    }
}

module.exports = EventScraper;