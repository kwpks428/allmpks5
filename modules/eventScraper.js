const { ethers } = require('ethers');
const moment = require('moment-timezone');

// 🧹 日誌清理：環境變量控制
const ENABLE_VERBOSE = process.env.ENABLE_VERBOSE_LOGS === 'true';
const ENABLE_DEBUG = process.env.ENABLE_DEBUG_LOGS === 'true';

// 簡化日誌函數
function verboseLog(...args) {
    if (ENABLE_VERBOSE) console.log(...args);
}

function debugLog(...args) {
    if (ENABLE_DEBUG) console.log(...args);
}

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

        // 🚀 RPC 優化：區塊範圍緩存
        this.blockRangeCache = new Map();
        this.cacheExpiry = 30 * 60 * 1000; // 30分鐘緩存（減少重複定位）

        // 🚀 RPC 優化：區塊時間戳緩存
        this.blockTimestampCache = new Map();
        this.timestampCacheExpiry = 60 * 60 * 1000; // 60分鐘緩存（時間戳變化不大）

        // 🚀 RPC 優化：批量請求配置
        this.batchSize = 200; // 單批查區塊時間戳擴大，降低往返次數

        // 🚀 RPC 優化：區塊範圍預熱機制
        this.blockRangePrewarm = new Map();
        this.prewarmBatchSize = 5; // 降低預熱負擔，避免啟動時暴衝
        this.prewarmEnabled = false; // 預熱開關（關閉以避免背景RPC膨脹）
    }

    /**
     * 重置RPC調用統計
     */
    resetRpcStats() {
        this.rpcCallCount = 0;
        this.lastResetTime = Date.now();
    }

    /**
     * 🚀 RPC 優化：清理過期的緩存
     */
    cleanExpiredCache() {
        const now = Date.now();

        // 清理區塊範圍緩存
        for (const [key, value] of this.blockRangeCache.entries()) {
            if (now - value.timestamp > this.cacheExpiry) {
                this.blockRangeCache.delete(key);
            }
        }

        // 清理區塊時間戳緩存
        for (const [key, value] of this.blockTimestampCache.entries()) {
            if (now - value.timestamp > this.timestampCacheExpiry) {
                this.blockTimestampCache.delete(key);
            }
        }
    }

    /**
     * 🚀 RPC 優化：獲取區塊範圍緩存
     */
    getCachedBlockRange(epoch) {
        const cached = this.blockRangeCache.get(epoch);
        if (cached && Date.now() - cached.timestamp < this.cacheExpiry) {
            return cached.data;
        }
        return null;
    }

    /**
     * 🚀 RPC 優化：設置區塊範圍緩存
     */
    setCachedBlockRange(epoch, data) {
        this.blockRangeCache.set(epoch, {
            data,
            timestamp: Date.now()
        });
    }

    /**
     * 🚀 RPC 優化：批量獲取區塊時間戳
     */
    async getBlockTimestampsBatch(blockNumbers) {
        const now = Date.now();
        const uncachedBlocks = [];
        const result = new Map();

        // 檢查緩存
        for (const blockNum of blockNumbers) {
            const cached = this.blockTimestampCache.get(blockNum);
            if (cached && now - cached.timestamp < this.timestampCacheExpiry) {
                result.set(blockNum, cached.timestamp);
            } else {
                uncachedBlocks.push(blockNum);
            }
        }

        // 批量獲取未緩存的區塊
        if (uncachedBlocks.length > 0) {
            verboseLog(`   📦 批量獲取 ${uncachedBlocks.length} 個區塊時間戳...`);

            // 分批處理，避免單次請求過大
            for (let i = 0; i < uncachedBlocks.length; i += this.batchSize) {
                const batch = uncachedBlocks.slice(i, i + this.batchSize);
                const batchPromises = batch.map(async (blockNum) => {
                    try {
                        this.trackRpcCall();
                        const block = await this.provider.getBlock(blockNum);
                        return { blockNum, timestamp: block.timestamp };
                    } catch (error) {
                        console.warn(`   ⚠️ 獲取區塊 ${blockNum} 時間戳失敗: ${error.message}`);
                        return { blockNum, timestamp: Math.floor(Date.now() / 1000) };
                    }
                });

                const batchResults = await Promise.all(batchPromises);

                // 更新結果和緩存
                for (const { blockNum, timestamp } of batchResults) {
                    result.set(blockNum, timestamp);
                    this.blockTimestampCache.set(blockNum, {
                        timestamp,
                        cachedAt: now
                    });
                }
            }
        }

        return result;
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
            const epochNum = Number(currentEpoch);
            verboseLog(`📊 當前最新局次: ${epochNum}`);

            // 🚀 RPC 優化：獲取當前局次後自動開始預熱
            if (this.prewarmEnabled && !this.blockRangePrewarm.has('started')) {
                this.blockRangePrewarm.set('started', true);
                this.prewarmBlockRanges();
            }

            return epochNum;
        } catch (error) {
            console.error('❌ 獲取當前局次失敗:', error);
            throw error;
        }
    }

    /**
     * 🎯 核心方法：嚴格按照時間範圍獲取區塊範圍 (優化版)
     * 策略：當前局次開始時間 -> 下一局開始時間
     * 🚀 RPC 優化：添加緩存機制，減少重複請求
     * @param {number} epoch 局次編號
     * @returns {Promise<Object>} 區塊範圍 {from, to, timeRange}
     */
    async getBlockRangeForEpoch(epoch) {
        try {
            // 🚀 RPC 優化：檢查緩存
            const cached = this.getCachedBlockRange(epoch);
            if (cached) {
                console.log(`🔍 為局次 ${epoch} 獲取區塊範圍 (從緩存)...`);
                console.log(`✅ 局次 ${epoch} 區塊範圍確定 (緩存):`);
                console.log(`   📍 起始區塊: ${cached.from}`);
                console.log(`   📍 結束區塊: ${cached.to}`);
                console.log(`   📊 區塊總數: ${(cached.to - cached.from + 1).toLocaleString()}`);
                console.log(`   🚀 RPC調用: 0 次 (使用緩存)`);
                return cached;
            }

            console.log(`🔍 為局次 ${epoch} 獲取區塊範圍...`);

            // 清理過期緩存
            this.cleanExpiredCache();

            // 1. 獲取當前局次的時間戳信息
            this.trackRpcCall();
            const currentRoundInfo = await this.contract.rounds(epoch);
            const startTime = Number(currentRoundInfo.startTimestamp);

            if (startTime === 0) {
                throw new Error(`局次 ${epoch} 尚未開始或無效`);
            }

            verboseLog(`⏰ 局次 ${epoch} 開始時間: ${new Date(startTime * 1000).toISOString()}`);

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
                    verboseLog(`⏰ 局次 ${epoch + 1} 開始時間: ${new Date(endTime * 1000).toISOString()}`);
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
            verboseLog(`⏱️ 時間範圍: ${duration} 秒 (${Math.floor(duration / 60)} 分鐘)`);

            // 4. 輕量：使用「初猜 + 微調」找到區塊範圍（避免重型二分搜索）
            console.log(`🎯 使用輕量微調定位區塊...`);

            const [startBlock, endBlock] = await Promise.all([
                this.findBlockForTime(startTime, 'gte'),
                this.findBlockForTime(endTime, 'lt')
            ]);

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

            const result = {
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

            // 🚀 RPC 優化：緩存結果
            this.setCachedBlockRange(epoch, result);

            return result;

        } catch (error) {
            console.error(`❌ 為局次 ${epoch} 獲取區塊範圍失敗:`, error);
            throw error;
        }
    }

    /**
     * 🎯 精確的時間戳到區塊號轉換 (優化版)
     * 🚀 RPC 優化：使用更高效的搜索算法，減少RPC調用
     * @param {number} targetTime 目標時間戳
     * @param {string} type 搜索類型: 'start' | 'end'
     * @returns {Promise<number>} 區塊號
     */
    // 🎯 輕量版：時間對區塊定位（初猜 + 微調，最多少量 getBlock）
    async findBlockForTime(targetTime, mode = 'gte') {
        const isGte = mode === 'gte';
        // 1) 取得最新區塊，作為邊界與回退保護
        this.trackRpcCall();
        const latest = await this.provider.getBlockNumber();

        // 2) 從快取推估初值：使用最近一筆範圍（from/to）線性外插
        let guess = null;
        const cachedRanges = Array.from(this.blockRangeCache.values())
            .map(e => e.data)
            .filter(e => e && e.timeRange)
            .sort((a, b) => b.timeRange.startTime - a.timeRange.startTime);
        if (cachedRanges.length > 0) {
            const ref = cachedRanges[0];
            const refTime = ref.timeRange.startTime;
            const refBlock = ref.from;
            // 使用保守 blocksPerEpoch 估算（約 110 blocks / 300s ≈ 0.3667 bps）
            const bps = 110 / 300;
            const delta = Math.floor((targetTime - refTime) * bps);
            guess = Math.max(0, Math.min(latest, refBlock + delta));
        } else {
            // 沒快取時，保守地用 latest - 500k 作左界，線性回推
            const bps = 110 / 300;
            guess = Math.max(0, Math.min(latest, Math.floor(latest - (60 * 60 * 24) * bps))); // 約回推一天
        }

        // 3) 微調：固定步進 ΔBlocks，最多 3 次；必要時 1 次二分收尾
        const step = 100; // 可調 50~150
        let block = guess;
        let attempts = 0;
        let lastTime = null;

        // 先讀取 guess 的時間
        try {
            this.trackRpcCall();
            lastTime = (await this.provider.getBlock(block)).timestamp;
        } catch (e) {
            // 若失敗，調整到 latest 再試
            block = Math.min(block + step, latest);
            this.trackRpcCall();
            lastTime = (await this.provider.getBlock(block)).timestamp;
        }

        while (attempts < 3) {
            attempts++;
            if (isGte) {
                if (lastTime >= targetTime) {
                    // 嘗試往前逼近
                    const prev = Math.max(0, block - step);
                    this.trackRpcCall();
                    const t = (await this.provider.getBlock(prev)).timestamp;
                    if (t >= targetTime) {
                        block = prev; lastTime = t; continue;
                    }
                    // 上一個已經 < 目標，當前就是第一個 >=
                    break;
                } else {
                    // 還太早，往後移動
                    const next = Math.min(latest, block + step);
                    this.trackRpcCall();
                    const t = (await this.provider.getBlock(next)).timestamp;
                    block = next; lastTime = t; continue;
                }
            } else {
                // mode = 'lt'
                if (lastTime < targetTime) {
                    // 往後試探，看看是否仍 < 目標
                    const next = Math.min(latest, block + step);
                    this.trackRpcCall();
                    const t = (await this.provider.getBlock(next)).timestamp;
                    if (t < targetTime) { block = next; lastTime = t; continue; }
                    // 下一個已經 >= 目標，當前就是最後一個 <
                    break;
                } else {
                    // 時間太晚了，往前退
                    const prev = Math.max(0, block - step);
                    this.trackRpcCall();
                    const t = (await this.provider.getBlock(prev)).timestamp;
                    block = prev; lastTime = t; continue;
                }
            }
        }

        // 4) 如仍不確定，做一次小範圍二分（最多 2 次）
        let left = Math.max(0, block - step);
        let right = Math.min(latest, block + step);
        let iterations = 0;
        while (iterations < 2 && left <= right) {
            iterations++;
            const mid = Math.floor((left + right) / 2);
            this.trackRpcCall();
            const midTime = (await this.provider.getBlock(mid)).timestamp;
            if (isGte) {
                if (midTime >= targetTime) { right = mid - 1; block = mid; lastTime = midTime; }
                else { left = mid + 1; }
            } else {
                if (midTime < targetTime) { left = mid + 1; block = mid; lastTime = midTime; }
                else { right = mid - 1; }
            }
        }

        // 邊界修正：確保滿足條件
        if (isGte) {
            // 確保第一個 >= targetTime
            while (block > 0) {
                const prev = block - 1;
                this.trackRpcCall();
                const t = (await this.provider.getBlock(prev)).timestamp;
                if (t >= targetTime) { block = prev; lastTime = t; }
                else break;
                if (block % step === 0) break; // 避免向左掃描過久
            }
        } else {
            // 確保最後一個 < targetTime
            while (block < latest) {
                const next = block + 1;
                this.trackRpcCall();
                const t = (await this.provider.getBlock(next)).timestamp;
                if (t < targetTime) { block = next; lastTime = t; }
                else break;
                if ((next - guess) > step) break; // 避免向右掃描過久
            }
        }

        return block;
    }

    async findExactBlockByTimestamp(targetTime, type = 'start') {
        return this.findExactBlockByTimestampOptimized(targetTime, type);
    }

    /**
     * 🚀 RPC 優化：超級優化的二分搜索算法
     * 目標：將 RPC 調用次數減少到 50-100 次以內
     * 策略：多階段搜索 + 更精確的估算 + 區塊範圍預熱
     */
    async findExactBlockByTimestampOptimized(targetTime, type = 'start', initialGuess = null) {
        const isStartSearch = type === 'start';
        const searchDesc = isStartSearch ? '第一個 >= 目標時間' : '最後一個 < 目標時間';

        verboseLog(`🔍 超級二分搜索: 尋找${searchDesc}的區塊 (目標: ${new Date(targetTime * 1000).toISOString()})`);

        this.trackRpcCall();
        const latestBlock = await this.provider.getBlockNumber();

        let left = Math.max(0, latestBlock - 5_000_000); // 限縮到近期 5M 區塊
        let right = latestBlock;
        let result = isStartSearch ? latestBlock : 0;
        let iterations = 0;
        let rpcCalls = 0;

        // 🚀 階段1：粗略估算，使用更大的步長快速縮小範圍
        verboseLog(`   📊 階段1: 粗略估算範圍...`);

        // 獲取邊界時間戳
        let leftTime, rightTime;
        try {
            this.trackRpcCall(); rpcCalls++;
            const [leftBlock, rightBlock] = await Promise.all([
                this.provider.getBlock(left),
                this.provider.getBlock(right)
            ]);
            leftTime = leftBlock.timestamp;
            rightTime = rightBlock.timestamp;
        } catch (error) {
            console.warn(`   ⚠️ 獲取邊界區塊時間戳失敗: ${error.message}`);
            leftTime = 0;
            rightTime = Math.floor(Date.now() / 1000);
        }

        // 🚀 粗略估算：優先使用智能估算，然後使用樣本點進行更精確估算
        if (targetTime >= leftTime && targetTime <= rightTime) {
            const timeRange = rightTime - leftTime;
            const blockRange = right - left;

            // 首先嘗試智能估算
            const smartEstimate = this.getSmartBlockEstimate(targetTime);
            let initialEstimate = null;

            if (smartEstimate && smartEstimate.confidence > 0.3) {
                initialEstimate = smartEstimate.estimatedBlock;
                console.log(`   📊 智能估算: 區塊 ${initialEstimate}, 置信度 ${(smartEstimate.confidence * 100).toFixed(1)}%`);
            }

            // 使用多個樣本點進行線性回歸估算
            const samplePoints = 5;
            const sampleBlocks = [];
            const sampleTimes = [];

            // 如果有智能估算，優先在估算位置附近取樣
            if (initialEstimate && initialEstimate > left && initialEstimate < right) {
                const sampleRange = Math.floor(blockRange / samplePoints);
                for (let i = 0; i < samplePoints; i++) {
                    const offset = (i - 2) * sampleRange; // -2, -1, 0, 1, 2
                    const sampleBlock = Math.max(left, Math.min(right, initialEstimate + offset));
                    sampleBlocks.push(sampleBlock);
                }
            } else {
                // 回退到均勻取樣
                for (let i = 0; i < samplePoints; i++) {
                    const sampleBlock = left + Math.floor((blockRange * (i + 1)) / (samplePoints + 1));
                    sampleBlocks.push(sampleBlock);
                }
            }

            // 批量獲取樣本區塊時間戳
            try {
                this.trackRpcCall(); rpcCalls++;
                const sampleBlockData = await Promise.all(
                    sampleBlocks.map(blockNum => this.provider.getBlock(blockNum))
                );

                sampleBlockData.forEach(blockData => {
                    sampleTimes.push(blockData.timestamp);
                });

                // 使用線性回歸計算更精確的估算位置
                let sumX = 0, sumY = 0, sumXY = 0, sumXX = 0;
                for (let i = 0; i < samplePoints; i++) {
                    const x = sampleBlocks[i];
                    const y = sampleTimes[i];
                    sumX += x;
                    sumY += y;
                    sumXY += x * y;
                    sumXX += x * x;
                }

                const slope = (samplePoints * sumXY - sumX * sumY) / (samplePoints * sumXX - sumX * sumX);
                const intercept = (sumY - slope * sumX) / samplePoints;

                const estimatedPosition = Math.floor((targetTime - intercept) / slope);
                const mid = Math.max(left, Math.min(right, estimatedPosition));

                // 驗證估算位置
                this.trackRpcCall(); rpcCalls++;
                const midBlock = await this.provider.getBlock(mid);
                const midTime = midBlock.timestamp;

                verboseLog(`   📊 樣本估算: 區塊 ${mid}, 時間 ${new Date(midTime * 1000).toISOString()}, 誤差 ${Math.abs(midTime - targetTime)}s`);

                // 根據估算結果調整搜索範圍
                if (midTime < targetTime) {
                    left = mid;
                    leftTime = midTime;
                } else {
                    right = mid;
                    rightTime = midTime;
                }

            } catch (error) {
                console.warn(`   ⚠️ 樣本估算失敗: ${error.message}`);
                // 回退到簡單估算
                const estimatedPosition = Math.floor(left + (right - left) * (targetTime - leftTime) / (rightTime - leftTime));
                const mid = Math.max(left, Math.min(right, estimatedPosition));

                if (mid > left && mid < right) {
                    try {
                        this.trackRpcCall(); rpcCalls++;
                        const midBlock = await this.provider.getBlock(mid);
                        const midTime = midBlock.timestamp;

                        if (midTime < targetTime) {
                            left = mid;
                        } else {
                            right = mid;
                        }
                    } catch (error) {
                        console.warn(`   ⚠️ 簡單估算失敗: ${error.message}`);
                    }
                }
            }
        }

        verboseLog(`   📊 粗略範圍縮小到: ${left} - ${right} (${right - left + 1} 個區塊), RPC調用: ${rpcCalls}`);

        // 🚀 階段2：精細二分搜索，使用更小的步長
        verboseLog(`   📊 階段2: 精細二分搜索...`);

        const maxIterations = Math.min(24, Math.ceil(Math.log2(Math.max(1, right - left))) + 6); // 進一步限制迭代
        const logInterval = Math.max(5, Math.floor(maxIterations / 8));

        while (left <= right && iterations < maxIterations) {
            iterations++;
            let mid;
            if (initialGuess && iterations === 1) {
                mid = Math.max(left, Math.min(right, initialGuess));
            } else {
                mid = Math.floor((left + right) / 2);
            }

            try {
                this.trackRpcCall(); rpcCalls++;
                const block = await this.provider.getBlock(mid);
                const blockTime = block.timestamp;

                // 減少日誌輸出
                if (iterations % logInterval === 0 || right - left < 50) {
                    debugLog(`   📊 迭代 ${iterations}: 區塊 ${mid}, 時間差 ${blockTime - targetTime}s, 範圍 ${right - left + 1}`);
                }

                if (isStartSearch) {
                    if (blockTime >= targetTime) {
                        result = mid;
                        right = mid - 1;
                    } else {
                        left = mid + 1;
                    }
                } else {
                    if (blockTime < targetTime) {
                        result = mid;
                        left = mid + 1;
                    } else {
                        right = mid - 1;
                    }
                }

                // 提前終止條件：範圍已經很小
                if (right - left < 50) {
                    verboseLog(`   📊 範圍已縮小到 ${right - left + 1} 個區塊，提前終止搜索`);
                    break;
                }

            } catch (error) {
                console.warn(`   ⚠️ 獲取區塊 ${mid} 失敗: ${error.message}`);
                // 出錯時保守地縮小範圍
                if (isStartSearch) {
                    right = mid - 1;
                } else {
                    left = mid + 1;
                }
            }
        }

        // 🚀 階段3：最終驗證和微調
        verboseLog(`   📊 階段3: 最終驗證...`);

        try {
            this.trackRpcCall(); rpcCalls++;
            const resultBlock = await this.provider.getBlock(result);
            const timeDiff = (resultBlock?.timestamp ?? targetTime) - targetTime;

            console.log(`   ✅ 搜索完成: 區塊 ${result}, 時間差 ${timeDiff}s, 總迭代 ${iterations} 次, 總RPC調用 ${rpcCalls} 次`);

            // 微調：如果時間差太大，嘗試找更好的區塊
            if (isStartSearch && timeDiff < -120) { // 開始搜索允許稍微早一點
                // 檢查下一個區塊是否更好
                try {
                    this.trackRpcCall(); rpcCalls++;
                    const nextBlock = await this.provider.getBlock(result + 1);
                    if (nextBlock.timestamp >= targetTime && Math.abs(nextBlock.timestamp - targetTime) < Math.abs(timeDiff)) {
                        result = result + 1;
                        console.log(`   🔄 微調: 使用區塊 ${result} (更好的時間匹配)`);
                    }
                } catch (error) {
                    // 忽略微調失敗
                }
            } else if (!isStartSearch && timeDiff > 120) { // 結束搜索允許稍微晚一點
                // 檢查前一個區塊是否更好
                try {
                    this.trackRpcCall(); rpcCalls++;
                    const prevBlock = await this.provider.getBlock(result - 1);
                    if (prevBlock.timestamp < targetTime && Math.abs(prevBlock.timestamp - targetTime) < Math.abs(timeDiff)) {
                        result = result - 1;
                        console.log(`   🔄 微調: 使用區塊 ${result} (更好的時間匹配)`);
                    }
                } catch (error) {
                    // 忽略微調失敗
                }
            }

            if (isStartSearch && timeDiff < -300) {
                console.warn(`   ⚠️ 警告: 開始區塊時間比目標時間早 ${-timeDiff} 秒`);
            } else if (!isStartSearch && timeDiff > 300) {
                console.warn(`   ⚠️ 警告: 結束區塊時間比目標時間晚 ${timeDiff} 秒`);
            }

        } catch (error) {
            console.warn(`   ⚠️ 無法驗證結果區塊 ${result}: ${error.message}`);
        }

        return result;
    }

    /**
     * 批量抓取指定區塊範圍內的所有事件 (優化版)
     * 🚀 RPC 優化：智能分批處理，避免RPC限制，減少總調用次數
     * @param {number} fromBlock 起始區塊
     * @param {number} toBlock 結束區塊
     * @returns {Promise<Object>} 事件數據
     */
    async fetchEventsInRange(fromBlock, toBlock) {
        try {
            const blockCount = toBlock - fromBlock + 1;
            console.log(`📊 開始抓取區塊範圍 ${fromBlock.toLocaleString()} - ${toBlock.toLocaleString()} (${blockCount.toLocaleString()} 個區塊)`);

            const events = {
                // 我們只關注下注與領獎
                startRoundEvents: [],
                lockRoundEvents: [],
                endRoundEvents: [],
                betBullEvents: [],
                betBearEvents: [],
                claimEvents: [],
                totalEvents: 0
            };

            // 🚀 RPC 優化：智能分批處理
            const maxBlocksPerBatch = 100000; // 每個批次最大區塊數
            const totalBatches = Math.ceil(blockCount / maxBlocksPerBatch);

            // 對相鄰 epoch 復用範圍：若 fromBlock 與 toBlock 差距 < 1500，直接單批處理避免分批浪費

            if (totalBatches > 1) {
                console.log(`📦 區塊範圍較大，分 ${totalBatches} 個批次處理，每批最多 ${maxBlocksPerBatch.toLocaleString()} 個區塊`);

                // 分批處理
                for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
                    const batchStart = fromBlock + (batchIndex * maxBlocksPerBatch);
                    const batchEnd = Math.min(toBlock, batchStart + maxBlocksPerBatch - 1);

                    console.log(`   📦 處理批次 ${batchIndex + 1}/${totalBatches}: ${batchStart.toLocaleString()} - ${batchEnd.toLocaleString()}`);

                    const batchEvents = await this.fetchEventsInBatch(batchStart, batchEnd);

                    // 合併批次結果
                    events.startRoundEvents.push(...batchEvents.startRoundEvents);
                    events.lockRoundEvents.push(...batchEvents.lockRoundEvents);
                    events.endRoundEvents.push(...batchEvents.endRoundEvents);
                    events.betBullEvents.push(...batchEvents.betBullEvents);
                    events.betBearEvents.push(...batchEvents.betBearEvents);
                    events.claimEvents.push(...batchEvents.claimEvents);
                }
            } else {
                // 單批次處理
                const batchEvents = await this.fetchEventsInBatch(fromBlock, toBlock);
                events.startRoundEvents = batchEvents.startRoundEvents;
                events.lockRoundEvents = batchEvents.lockRoundEvents;
                events.endRoundEvents = batchEvents.endRoundEvents;
                events.betBullEvents = batchEvents.betBullEvents;
                events.betBearEvents = batchEvents.betBearEvents;
                events.claimEvents = batchEvents.claimEvents;
            }

            events.totalEvents = events.startRoundEvents.length + events.lockRoundEvents.length +
                events.endRoundEvents.length + events.betBullEvents.length +
                events.betBearEvents.length + events.claimEvents.length;

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
     * 🚀 RPC 優化：單批次事件抓取
     */
    async fetchEventsInBatch(fromBlock, toBlock) {
        // 並行抓取所有事件類型
        const [
            betBullEvents,
            betBearEvents,
            claimEvents
        ] = await Promise.all([
            this.fetchEventsByFilter('BetBull', this.filters.betBull, fromBlock, toBlock),
            this.fetchEventsByFilter('BetBear', this.filters.betBear, fromBlock, toBlock),
            this.fetchEventsByFilter('Claim', this.filters.claim, fromBlock, toBlock)
        ]);

        return {
            startRoundEvents: [],
            lockRoundEvents: [],
            endRoundEvents: [],
            betBullEvents,
            betBearEvents,
            claimEvents
        };
    }

    /**
     * 🎯 修復版：按事件類型抓取 - 現在會獲取真實時間戳
     */
    async fetchEventsByFilter(eventName, filter, fromBlock, toBlock) {
        try {
            this.trackRpcCall();
            // 若範圍過大，先切割成較小片段聚合（降低單次 provider 壓力）
            const sliceSize = Number(process.env.SLICE_SIZE) || 20_000;
            const results = [];
            let cursor = fromBlock;
            while (cursor <= toBlock) {
                const end = Math.min(toBlock, cursor + sliceSize - 1);
                this.trackRpcCall();
                const part = await this.contract.queryFilter(filter, cursor, end);
                results.push(...part);
                cursor = end + 1;
                // 輕微節流，避免節點限流導致隱性重試
                const sleepMs = Number(process.env.SLICE_SLEEP_MS) || 180;
                await new Promise(r => setTimeout(r, sleepMs));
            }
            const rawEvents = results;
            return await this.parseEvents(rawEvents, eventName); // 🎯 改為 await
        } catch (error) {
            console.warn(`⚠️ 抓取 ${eventName} 事件失敗 (區塊 ${fromBlock}-${toBlock}):`, error.message);
            return [];
        }
    }

    /**
     * 🎯 修復版：解析原始事件數據並獲取真實時間戳 (優化版)
     * 🚀 RPC 優化：使用批量區塊時間戳獲取，大幅減少RPC調用
     * @param {Array} rawEvents 原始事件數組
     * @param {string} eventType 事件類型
     * @returns {Promise<Array>} 解析後的事件數組
     */
    async parseEvents(rawEvents, eventType) {
        if (!rawEvents || rawEvents.length === 0) {
            return [];
        }

        const parsedEvents = [];

        // 🚀 RPC 優化：批量獲取區塊時間戳，使用優化的批量方法
        const blockNumbers = [...new Set(rawEvents.map(event => event.blockNumber))];
        verboseLog(`   📅 獲取 ${blockNumbers.length} 個區塊的時間戳 (${eventType})...`);

        const blockTimestamps = await this.getBlockTimestampsBatch(blockNumbers);

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
     * 獲取指定局次的完整事件數據 (優化版)
     * 🚀 RPC 優化：整合所有優化策略，提供最佳性能
     */
    async getEventsForEpoch(epoch) {
        try {
            console.log(`🎯 開始獲取局次 ${epoch} 的事件數據...`);
            this.resetRpcStats();

            // 🚀 RPC 優化：預計算相鄰局次的區塊範圍
            this.precalculateAdjacentEpochs(epoch);

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
     * 🚀 RPC 優化：預計算相鄰局次的區塊範圍
     * 提前計算和緩存相鄰局次，減少後續請求
     */
    async precalculateAdjacentEpochs(currentEpoch) { /* 關閉以避免背景RPC膨脹 */ return; 
        const adjacentEpochs = [
            currentEpoch - 1,
            currentEpoch + 1,
            currentEpoch - 2,
            currentEpoch + 2
        ].filter(epoch => epoch > 0);

        // 異步預計算，不阻塞當前請求
        setImmediate(async () => {
            for (const epoch of adjacentEpochs) {
                try {
                    // 只預計算未緩存的
                    if (!this.getCachedBlockRange(epoch)) {
                        await this.getBlockRangeForEpoch(epoch);
                    }
                } catch (error) {
                    // 預計算失敗不影響主流程
                    console.debug(`預計算局次 ${epoch} 區塊範圍失敗: ${error.message}`);
                }
            }
        });
    }

    /**
     * 🚀 RPC 優化：區塊範圍預熱機制
     * 在系統啟動時預先計算常用區塊範圍，減少首次請求延遲
     */
    async prewarmBlockRanges() {
        if (!this.prewarmEnabled) {
            verboseLog('🚀 區塊範圍預熱已禁用');
            return;
        }

        try {
            verboseLog('🚀 開始區塊範圍預熱...');

            this.trackRpcCall();
            const currentEpoch = await this.contract.currentEpoch();
            const currentEpochNum = Number(currentEpoch);

            // 預熱最近的 N 個局次
            const epochsToPrewarm = [];
            for (let i = 0; i < this.prewarmBatchSize; i++) {
                const epoch = currentEpochNum - i;
                if (epoch > 0) {
                    epochsToPrewarm.push(epoch);
                }
            }

            verboseLog(`🚀 預熱 ${epochsToPrewarm.length} 個局次的區塊範圍...`);

            // 批量預熱，但不要阻塞主線程
            setImmediate(async () => {
                let prewarmed = 0;
                let skipped = 0;

                for (const epoch of epochsToPrewarm) {
                    try {
                        if (!this.getCachedBlockRange(epoch)) {
                            await this.getBlockRangeForEpoch(epoch);
                            prewarmed++;
                        } else {
                            skipped++;
                        }
                    } catch (error) {
                        console.debug(`預熱局次 ${epoch} 失敗: ${error.message}`);
                    }
                }

                console.log(`✅ 區塊範圍預熱完成: 新預熱 ${prewarmed} 個, 跳過 ${skipped} 個已緩存`);
            });

        } catch (error) {
            console.warn('⚠️ 區塊範圍預熱失敗:', error.message);
        }
    }

    /**
     * 🚀 RPC 優化：智能區塊估算算法
     * 使用歷史數據和趨勢分析提供更精確的區塊估算
     */
    getSmartBlockEstimate(targetTime) {
        // 獲取最近的區塊範圍緩存作為估算依據
        const cachedRanges = Array.from(this.blockRangeCache.values())
            .map(entry => entry.data)
            .filter(range => range && range.timeRange)
            .sort((a, b) => b.timeRange.startTime - a.timeRange.startTime) // 按時間降序
            .slice(0, 5); // 只用最近5個

        if (cachedRanges.length < 2) {
            return null; // 沒有足夠數據進行智能估算
        }

        // 計算區塊時間間隔趨勢
        const trends = [];
        for (let i = 0; i < cachedRanges.length - 1; i++) {
            const current = cachedRanges[i];
            const previous = cachedRanges[i + 1];

            if (current.timeRange && previous.timeRange) {
                const timeDiff = current.timeRange.startTime - previous.timeRange.startTime;
                const blockDiff = current.from - previous.from;

                if (timeDiff > 0 && blockDiff > 0) {
                    const blocksPerSecond = blockDiff / timeDiff;
                    trends.push({
                        blocksPerSecond,
                        weight: 1 / (i + 1) // 越近的數據權重越大
                    });
                }
            }
        }

        if (trends.length === 0) {
            return null;
        }

        // 加權平均計算區塊生成速率
        let totalWeight = 0;
        let weightedSum = 0;

        trends.forEach(trend => {
            weightedSum += trend.blocksPerSecond * trend.weight;
            totalWeight += trend.weight;
        });

        const avgBlocksPerSecond = weightedSum / totalWeight;

        // 使用最近的區塊範圍作為基準點
        const reference = cachedRanges[0];
        const timeDiff = targetTime - reference.timeRange.startTime;
        const estimatedBlocks = Math.floor(timeDiff * avgBlocksPerSecond);
        const estimatedBlock = reference.from + estimatedBlocks;

        return {
            estimatedBlock,
            confidence: Math.min(trends.length / 5, 1), // 基於樣本數的置信度
            avgBlocksPerSecond,
            referenceEpoch: reference.timeRange ? 'unknown' : 'latest'
        };
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