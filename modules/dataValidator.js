const moment = require('moment-timezone');

/**
 * 數據驗證器 - 修復版
 * 正確區分 claim 表的 epoch 和 betEpoch 字段
 */
class DataValidator {
    constructor(timezone = 'Asia/Taipei') {
        this.timezone = timezone;
        this.requiredDecimals = 8;
        this.pricePrecision = 0.0001; // 價格精度
        this.amountPrecision = 0.00000001; // 金額精度 (1e-8 BNB)
        this.maxBetAmount = 1000; // 最大單次下注金額 (BNB)
        this.maxTotalAmount = 10000; // 最大總下注金額 (BNB)
    }

    /**
     * 驗證局次數據的完整性 - 修復版
     * @param {Object} eventsData 事件數據
     * @param {number} currentEpoch 當前處理的局次（用於區分epoch和betEpoch）
     * @returns {Promise<Object>} 驗證結果
     */
    async validateEpochData(eventsData, currentEpoch = null) {
        try {
            console.log('🔍 開始嚴格數據驗證...');

            // 🎯 如果沒有傳入 currentEpoch，從 StartRound 事件中獲取
            if (currentEpoch === null && eventsData.startRoundEvents.length > 0) {
                currentEpoch = eventsData.startRoundEvents[0].epoch;
            }

            const validationResult = {
                isValid: true,
                errors: [],
                warnings: [],
                roundData: null,
                hisBetData: [],
                claimData: [],
                stats: {},
                currentEpoch: currentEpoch
            };

            // 1. 驗證事件完整性
            this.validateEventsIntegrity(eventsData, validationResult);
            if (!validationResult.isValid) return validationResult;

            // 2. 驗證 round 數據
            const roundValidation = this.validateRoundData(eventsData);
            if (!roundValidation.isValid) {
                validationResult.errors.push(...roundValidation.errors);
                validationResult.isValid = false;
            }
            validationResult.roundData = roundValidation.data;
            validationResult.roundResult = roundValidation.roundResult || 'UP';

            // 3. 驗證 hisBet 數據
            const hisBetValidation = this.validateHisBetData(eventsData, validationResult.roundResult);
            if (!hisBetValidation.isValid) {
                validationResult.errors.push(...hisBetValidation.errors);
                validationResult.isValid = false;
            }
            validationResult.hisBetData = hisBetValidation.data;

            // 4. 🎯 修復版：驗證 claim 數據（正確區分 epoch 和 betEpoch）
            const claimValidation = this.validateClaimData(eventsData, currentEpoch);
            if (!claimValidation.isValid) {
                validationResult.errors.push(...claimValidation.errors);
                validationResult.isValid = false;
            }
            validationResult.claimData = claimValidation.data;

            // 5. 跨表數據一致性驗證
            this.validateDataConsistency(validationResult);

            // 生成統計信息
            validationResult.stats = this.generateStats(validationResult);

            if (validationResult.isValid) {
                console.log('✅ 嚴格數據驗證完成');
            } else {
                console.log('❌ 數據驗證失敗:', validationResult.errors);
            }

            return validationResult;

        } catch (error) {
            console.error('❌ 數據驗證過程中發生錯誤:', error);
            return {
                isValid: false,
                errors: [`驗證過程錯誤: ${error.message}`],
                warnings: [],
                roundData: null,
                hisBetData: [],
                claimData: [],
                stats: {}
            };
        }
    }

    /**
     * 驗證事件完整性 - 嚴格版
     */
    validateEventsIntegrity(eventsData, result) {
        // 必須有 StartRound 事件
        if (!eventsData.startRoundEvents || eventsData.startRoundEvents.length === 0) {
            result.errors.push('缺少 StartRound 事件');
            result.isValid = false;
            return;
        }

        const startRound = eventsData.startRoundEvents[0];
        if (!startRound.epoch) {
            result.errors.push('StartRound 事件缺少 epoch 信息');
            result.isValid = false;
            return;
        }

        // 統計事件信息
        console.log('🔍 事件統計:', {
            startRoundEvents: eventsData.startRoundEvents.length,
            lockRoundEvents: eventsData.lockRoundEvents.length,
            endRoundEvents: eventsData.endRoundEvents.length,
            betBullEvents: eventsData.betBullEvents.length,
            betBearEvents: eventsData.betBearEvents.length,
            claimEvents: eventsData.claimEvents.length
        });

        console.log('📊 第一個 StartRound 事件:', {
            epoch: startRound.epoch,
            blockNumber: startRound.blockNumber
        });

        // 驗證下注事件完整性
        const allBetEvents = [
            ...(eventsData.betBullEvents || []),
            ...(eventsData.betBearEvents || [])
        ];

        for (const bet of allBetEvents) {
            if (!bet.epoch || !bet.sender || bet.amount === undefined || bet.amount === null) {
                result.errors.push(`下注事件缺少必要信息: epoch=${bet.epoch}, sender=${bet.sender}, amount=${bet.amount}`);
                result.isValid = false;
            }
            if (typeof bet.amount !== 'number' || bet.amount <= 0) {
                result.errors.push(`下注金額無效: ${bet.amount}`);
                result.isValid = false;
            }
        }

        // 驗證 claim 事件完整性
        for (const claim of (eventsData.claimEvents || [])) {
            if (!claim.epoch || !claim.sender || claim.amount === undefined || claim.amount === null) {
                result.errors.push(`claim 事件缺少必要信息: epoch=${claim.epoch}, sender=${claim.sender}, amount=${claim.amount}`);
                result.isValid = false;
            }
            if (typeof claim.amount !== 'number' || claim.amount <= 0) {
                result.errors.push(`claim 金額無效: ${claim.amount}`);
                result.isValid = false;
            }
        }

        const targetEpoch = startRound.epoch;
        console.log(`📊 事件完整性验证: 目标局次 ${targetEpoch}, 下注 ${allBetEvents.length}, claim ${eventsData.claimEvents.length}`);
    }

    /**
     * 驗證 round 數據完整性 - 簡化版（不做時間邏輯驗證）
     */
    validateRoundData(eventsData) {
        const validationResult = {
            isValid: true,
            errors: [],
            data: null
        };

        try {
            // 檢查必要事件
            if (eventsData.startRoundEvents.length === 0) {
                validationResult.errors.push('缺少 StartRound 事件');
                validationResult.isValid = false;
                return validationResult;
            }

            // 使用第一個 StartRound 事件作為基準
            const startRound = eventsData.startRoundEvents[0];
            const baseEpoch = startRound.epoch;

            // 接受跨局次事件模式
            console.log('🔍 接受跨 Epoch 事件模式:', {
                baseEpoch: baseEpoch,
                startRoundEpoch: startRound.epoch,
                lockRoundEpochs: eventsData.lockRoundEvents.map(e => e.epoch),
                endRoundEpochs: eventsData.endRoundEvents.map(e => e.epoch),
                betEpochs: [...eventsData.betBullEvents, ...eventsData.betBearEvents].map(e => e.epoch)
            });

            console.log('✅ 接受跨 Epoch 事件模式成功');

            // 獲取對應事件（容錯處理）
            const lockRound = eventsData.lockRoundEvents[0]; // 取第一個
            const endRound = eventsData.endRoundEvents[0]; // 取第一個
            const epoch = baseEpoch;

            // 計算本局次的下注統計
            const epochBetBullEvents = eventsData.betBullEvents.filter(e => e.epoch === epoch);
            const epochBetBearEvents = eventsData.betBearEvents.filter(e => e.epoch === epoch);

            const upAmount = epochBetBullEvents.reduce((sum, e) => sum + (e.amount || 0), 0);
            const downAmount = epochBetBearEvents.reduce((sum, e) => sum + (e.amount || 0), 0);
            const totalAmount = upAmount + downAmount;

            // 計算賠率
            const poolAfterFee = totalAmount * 0.97;
            const upOdds = upAmount > 0 ? (poolAfterFee / upAmount) : 0;
            const downOdds = downAmount > 0 ? (poolAfterFee / downAmount) : 0;

            // 判斷結果（不驗證價格邏輯）
            let result = 'UP'; // 默認
            if (lockRound?.price && endRound?.price) {
                const lockPrice = parseFloat(lockRound.price);
                const closePrice = parseFloat(endRound.price);
                result = closePrice > lockPrice ? 'UP' : 'DOWN';
            }

            // 構建 round 數據
            const roundData = {
                epoch: epoch,
                startTime: this.formatTime(startRound.timestamp),
                lockTime: this.formatTime(lockRound?.timestamp),
                closeTime: this.formatTime(endRound?.timestamp),
                lockPrice: this.parsePrice(lockRound?.price || '0'),
                closePrice: this.parsePrice(endRound?.price || '0'),
                result: result,
                totalAmount: this.roundAmount(totalAmount),
                upAmount: this.roundAmount(upAmount),
                downAmount: this.roundAmount(downAmount),
                upOdds: this.roundOdds(upOdds),
                downOdds: this.roundOdds(downOdds)
            };

            validationResult.data = roundData;
            validationResult.roundResult = result;

        } catch (error) {
            validationResult.errors.push(`round 數據驗證錯誤: ${error.message}`);
            validationResult.isValid = false;
        }

        return validationResult;
    }

    /**
     * 驗證 hisBet 數據 - 嚴格版
     */
    validateHisBetData(eventsData, roundResult = 'UP') {
        const result = {
            isValid: true,
            errors: [],
            data: []
        };

        try {
            const allBetEvents = [
                ...eventsData.betBullEvents,
                ...eventsData.betBearEvents
            ];

            if (allBetEvents.length === 0) {
                result.errors.push('沒有下注事件數據');
                result.isValid = false;
                return result;
            }

            const hisBetData = [];

            for (const event of allBetEvents) {
                // 嚴格驗證每個下注事件
                if (!event.sender || typeof event.sender !== 'string') {
                    result.errors.push('下注事件缺少有效的 sender 地址');
                    result.isValid = false;
                    continue;
                }

                if (!event.epoch || typeof event.epoch !== 'number') {
                    result.errors.push('下注事件缺少有效的 epoch');
                    result.isValid = false;
                    continue;
                }

                if (typeof event.amount !== 'number' || event.amount <= 0) {
                    result.errors.push(`下注事件金額無效: ${event.amount}`);
                    result.isValid = false;
                    continue;
                }

                // 判斷下注方向
                const isBullEvent = eventsData.betBullEvents.some(bullEvent =>
                    bullEvent.sender === event.sender &&
                    bullEvent.epoch === event.epoch &&
                    bullEvent.blockNumber === event.blockNumber &&
                    bullEvent.transactionHash === event.transactionHash
                );

                hisBetData.push({
                    epoch: event.epoch,
                    betTime: this.formatTime(event.timestamp),
                    walletAddress: event.sender.toLowerCase(),
                    betDirection: isBullEvent ? 'UP' : 'DOWN',
                    betAmount: this.roundAmount(event.amount),
                    result: this.calculateBetResult(isBullEvent ? 'UP' : 'DOWN', roundResult),
                    blockNumber: event.blockNumber || 0
                });
            }

            result.data = hisBetData;

        } catch (error) {
            result.errors.push(`hisBet 數據驗證錯誤: ${error.message}`);
            result.isValid = false;
        }

        return result;
    }

    /**
     * 🎯 修復版：驗證 claim 數據（正確區分 epoch 和 betEpoch）
     * @param {Object} eventsData 事件數據
     * @param {number} currentEpoch 當前處理的局次（觸發提領的局次）
     * @returns {Object} 驗證結果
     */
    validateClaimData(eventsData, currentEpoch) {
        const result = {
            isValid: true,
            errors: [],
            data: []
        };

        try {
            // claim 數據可以為空（某些局次可能沒有人提領）
            if (!eventsData.claimEvents || eventsData.claimEvents.length === 0) {
                console.log('📊 該局次沒有 claim 事件');
                return result;
            }

            const claimData = [];

            for (const event of eventsData.claimEvents) {
                // 嚴格驗證每個 claim 事件
                if (!event.sender || typeof event.sender !== 'string') {
                    result.errors.push('claim 事件缺少有效的 sender 地址');
                    result.isValid = false;
                    continue;
                }

                if (!event.epoch || typeof event.epoch !== 'number') {
                    result.errors.push('claim 事件缺少有效的 epoch (betEpoch)');
                    result.isValid = false;
                    continue;
                }

                if (typeof event.amount !== 'number' || event.amount <= 0) {
                    result.errors.push(`claim 事件金額無效: ${event.amount}`);
                    result.isValid = false;
                    continue;
                }

                // 🎯 關鍵修復：正確區分 epoch 和 betEpoch
                claimData.push({
                    epoch: currentEpoch,                    // 觸發提領的當前局次 (例如 426238)
                    walletAddress: event.sender.toLowerCase(),
                    betEpoch: event.epoch,                  // 實際獲勝的局次 (例如 426236)
                    claimAmount: this.roundAmount(event.amount)
                });
            }

            result.data = claimData;

            console.log(`📊 claim 數據驗證完成: ${claimData.length} 筆記錄`);
            if (claimData.length > 0) {
                const betEpochs = [...new Set(claimData.map(c => c.betEpoch))];
                console.log(`   🎯 涉及的獲勝局次 (betEpoch): ${betEpochs.join(', ')}`);
                console.log(`   📍 觸發局次 (epoch): ${currentEpoch}`);
            }

        } catch (error) {
            result.errors.push(`claim 數據驗證錯誤: ${error.message}`);
            result.isValid = false;
        }

        return result;
    }

    /**
     * 跨表數據一致性驗證
     */
    validateDataConsistency(result) {
        try {
            // 計算統計數據
            const stats = {
                totalBets: result.hisBetData.length,
                totalClaims: result.claimData.length,
                upBets: result.hisBetData.filter(b => b.betDirection === 'UP').length,
                downBets: result.hisBetData.filter(b => b.betDirection === 'DOWN').length,
                totalBetAmount: result.hisBetData.reduce((sum, b) => sum + b.betAmount, 0),
                totalClaimAmount: result.claimData.reduce((sum, c) => sum + c.claimAmount, 0)
            };

            // 驗證下注統計一致性
            if (stats.upBets + stats.downBets !== stats.totalBets) {
                result.errors.push(`下注統計不一致: ${stats.upBets} + ${stats.downBets} ≠ ${stats.totalBets}`);
                result.isValid = false;
            }

            // 驗證金額一致性（檢查小數點後4位是否相同）
            const roundedBetAmount = Math.round(stats.totalBetAmount * 10000) / 10000; // 四捨五入到4位小數
            const roundedRoundAmount = Math.round(result.roundData.totalAmount * 10000) / 10000;
            
            if (roundedBetAmount !== roundedRoundAmount) {
                result.errors.push(`總下注金額不一致 (4位小數檢查): hisBet=${stats.totalBetAmount}, round=${result.roundData.totalAmount}`);
                result.isValid = false;
            }

            // 驗證必須有下注數據
            if (stats.totalBets === 0) {
                result.errors.push(`局次 ${result.roundData.epoch} 沒有任何下注數據`);
                result.isValid = false;
            }

            // 驗證賠率合理性
            if (result.roundData.upOdds <= 0 && result.roundData.upAmount > 0) {
                result.errors.push(`Up方向有下注但賠率為0: upAmount=${result.roundData.upAmount}, upOdds=${result.roundData.upOdds}`);
                result.isValid = false;
            }

            if (result.roundData.downOdds <= 0 && result.roundData.downAmount > 0) {
                result.errors.push(`Down方向有下注但賠率為0: downAmount=${result.roundData.downAmount}, downOdds=${result.roundData.downOdds}`);
                result.isValid = false;
            }

            result.stats = stats;

            // 計算勝負分佈
            const winBets = result.hisBetData.filter(b =>
                (b.betDirection === 'UP' && result.roundData.result === 'UP') ||
                (b.betDirection === 'DOWN' && result.roundData.result === 'DOWN')
            ).length;
            const lossBets = result.hisBetData.length - winBets;

            console.log(`📊 數據一致性驗證完成:`);
            console.log(`   👥 總下注: ${stats.totalBets} 個 (UP: ${stats.upBets}, DOWN: ${stats.downBets})`);
            console.log(`   🎯 輸贏分佈: WIN: ${winBets}, LOSS: ${lossBets}`);
            console.log(`   💰 總金額: ${stats.totalBetAmount.toFixed(8)} BNB`);
            console.log(`   🏆 總獎勵: ${stats.totalClaimAmount.toFixed(8)} BNB`);
            console.log(`   📊 賠率: UP=${result.roundData.upOdds.toFixed(4)}, DOWN=${result.roundData.downOdds.toFixed(4)}`);
            console.log(`   🎮 遊戲結果: ${result.roundData.result.toUpperCase()}`);

        } catch (error) {
            result.errors.push(`數據一致性驗證錯誤: ${error.message}`);
            result.isValid = false;
        }
    }

    /**
     * 生成統計信息
     */
    generateStats(result) {
        try {
            const stats = {
                epoch: result.roundData.epoch,
                totalBets: result.hisBetData.length,
                totalClaims: result.claimData.length,
                upBets: result.hisBetData.filter(b => b.betDirection === 'up').length,
                downBets: result.hisBetData.filter(b => b.betDirection === 'down').length,
                totalBetAmount: result.hisBetData.reduce((sum, b) => sum + b.betAmount, 0),
                totalClaimAmount: result.claimData.reduce((sum, c) => sum + c.claimAmount, 0),
                gameResult: result.roundData.result,
                upOdds: result.roundData.upOdds,
                downOdds: result.roundData.downOdds,
                lockPrice: result.roundData.lockPrice,
                closePrice: result.roundData.closePrice,
                priceChange: result.roundData.closePrice - result.roundData.lockPrice,
                priceChangePercent: result.roundData.lockPrice > 0 ?
                    ((result.roundData.closePrice - result.roundData.lockPrice) / result.roundData.lockPrice * 100) : 0
            };

            // 計算勝負統計
            stats.winBets = result.hisBetData.filter(b =>
                (b.betDirection === 'UP' && stats.gameResult === 'UP') ||
                (b.betDirection === 'DOWN' && stats.gameResult === 'DOWN')
            ).length;
            stats.lossBets = stats.totalBets - stats.winBets;

            // 計算獲勝金額
            stats.winAmount = result.hisBetData
                .filter(b =>
                    (b.betDirection === 'UP' && stats.gameResult === 'UP') ||
                    (b.betDirection === 'DOWN' && stats.gameResult === 'DOWN')
                )
                .reduce((sum, b) => sum + b.betAmount, 0);
            stats.lossAmount = stats.totalBetAmount - stats.winAmount;

            return stats;

        } catch (error) {
            console.error('統計信息生成錯誤:', error);
            return {};
        }
    }

    // 工具方法
    formatTime(timestamp) {
        try {
            if (!timestamp || timestamp === 0) {
                console.warn(`無效的時間戳: ${timestamp}`);
                return moment().tz(this.timezone).format('YYYY-MM-DD HH:mm:ss');
            }

            if (typeof timestamp !== 'number') {
                console.warn(`時間戳類型錯誤: ${typeof timestamp}, 值: ${timestamp}`);
                const parsed = parseInt(timestamp);
                if (isNaN(parsed)) {
                    return moment().tz(this.timezone).format('YYYY-MM-DD HH:mm:ss');
                }
                timestamp = parsed;
            }

            return moment.unix(timestamp).tz(this.timezone).format('YYYY-MM-DD HH:mm:ss');
        } catch (error) {
            console.warn(`格式化時間失敗: ${error.message}, timestamp: ${timestamp}`);
            return moment().tz(this.timezone).format('YYYY-MM-DD HH:mm:ss');
        }
    }

    parsePrice(price) {
        try {
            if (price === null || price === undefined) return 0;

            const parsed = parseFloat(price.toString());
            if (isNaN(parsed)) {
                console.warn(`無效的價格數據: ${price}`);
                return 0;
            }

            if (parsed < 0) {
                console.warn(`負數價格: ${parsed}`);
                return 0;
            }

            return parsed;
        } catch (error) {
            console.warn(`解析價格失敗: ${error.message}, price: ${price}`);
            return 0;
        }
    }

    roundAmount(amount) {
        try {
            if (amount === null || amount === undefined) return 0;

            const parsed = parseFloat(amount);
            if (isNaN(parsed)) {
                console.warn(`無效的金額數據: ${amount}`);
                return 0;
            }

            if (parsed < 0) {
                console.warn(`負數金額: ${parsed}`);
                return 0;
            }

            // 保持8位小數精度
            return Math.round(parsed * 100000000) / 100000000;
        } catch (error) {
            console.warn(`處理金額失敗: ${error.message}, amount: ${amount}`);
            return 0;
        }
    }

    roundOdds(odds) {
        try {
            if (odds === null || odds === undefined) return 0;

            const parsed = parseFloat(odds);
            if (isNaN(parsed)) {
                console.warn(`無效的賠率數據: ${odds}`);
                return 0;
            }

            if (parsed < 0) {
                console.warn(`負數賠率: ${parsed}`);
                return 0;
            }

            // 保持4位小數精度
            return Math.round(parsed * 10000) / 10000;
        } catch (error) {
            console.warn(`處理賠率失敗: ${error.message}, odds: ${odds}`);
            return 0;
        }
    }

    /**
     * 驗證錢包地址格式
     */
    validateAddress(address) {
        if (!address || typeof address !== 'string') {
            return false;
        }

        // 基本的以太坊地址格式檢查
        const addressPattern = /^0x[a-fA-F0-9]{40}$/;
        return addressPattern.test(address);
    }

    /**
     * 驗證區塊號
     */
    validateBlockNumber(blockNumber) {
        return typeof blockNumber === 'number' && blockNumber > 0 && Number.isInteger(blockNumber);
    }

    /**
     * 計算下注結果
     * @param {string} betDirection 下注方向 ('UP' 或 'DOWN')
     * @param {string} gameResult 遊戲結果 ('UP' 或 'DOWN')
     * @returns {string} 下注結果 ('WIN' 或 'LOSS')
     */
    calculateBetResult(betDirection, gameResult) {
        return betDirection === gameResult ? 'WIN' : 'LOSS';
    }
}

module.exports = DataValidator;