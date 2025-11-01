#!/usr/bin/env node

/**
 * PancakeSwap BNB/USD 預測遊戲歷史數據抓取系統
 * 版本：v2.2 (含錯誤日誌)
 * 核心技術：Node.js + ethers.js + Redis + PostgreSQL
 * 作者：HisBet Team
 */

require('dotenv').config();
const moment = require('moment-timezone');

// 導入自定義模組
const Database = require('./modules/database');
const RedisLock = require('./modules/redisLock');
const EventScraper = require('./modules/eventScraper');
const DataValidator = require('./modules/dataValidator');
const TransactionManager = require('./modules/transactionManager');
const Scheduler = require('./modules/scheduler');
const Logger = require('./modules/logger');

class HisBetScraper {
    constructor() {
        this.config = {
            contractAddress: '0x18B2A687610328590Bc8F2e5fEdDe3b582A49cdA',
            rpcUrl: process.env.RPC_URL,
            wsRpcUrl: process.env.RPC_WS_URL,
            redisUrl: process.env.REDIS_URL,
            postgresUrl: process.env.POSTGRES_URL,
            timezone: 'Asia/Taipei',
            lockTimeout: 120, // 2分鐘
            mainThreadRestartInterval: 30 * 60 * 1000, // 30分鐘
            secondaryThreadInterval: 5 * 60 * 1000, // 5分鐘
            initialSecondaryThreadDelay: 5 * 60 * 1000 // 5分鐘
        };

        this.db = null;
        this.redis = null;
        this.eventScraper = null;
        this.dataValidator = null;
        this.transactionManager = null;
        this.scheduler = null;
        this.logger = null;

        this.isShuttingDown = false;
        this.currentEpoch = null;

        // 🚨 連續失敗監控機制
        this.consecutiveFailures = 0;
        this.maxConsecutiveFailures = 3; // 連續 3 次失敗就中斷系統
        this.failureWindowStart = null;
        this.failureWindowDuration = 10 * 60 * 1000; // 10 分鐘失敗窗口
    }

    /**
     * 初始化系統
     */
    async initialize() {
        try {
            // 初始化Logger
            this.logger = new Logger();
            this.logger.startup('HisBet 數據抓取系統');

            // 檢查環境變數
            this.logger.debug('🔍 檢查環境配置...');
            this.logger.debug('RPC_URL:', this.config.rpcUrl ? '✅' : '❌');
            this.logger.debug('REDIS_URL:', this.config.redisUrl ? '✅' : '❌');
            this.logger.debug('POSTGRES_URL:', this.config.postgresUrl ? '✅' : '❌');

            // 初始化資料庫
            this.logger.info('🔄 初始化資料庫...');
            this.db = new Database(this.config.postgresUrl);
            await this.db.connect();
            this.logger.success('✅ 資料庫連接成功');

            // 初始化Redis
            this.logger.info('🔄 初始化Redis...');
            this.redis = new RedisLock(this.config.redisUrl);
            await this.redis.connect();
            this.logger.success('✅ Redis 連接成功');

            // 初始化事件抓取器
            this.logger.info('🔄 初始化事件抓取器...');
            this.eventScraper = new EventScraper(
                this.config.rpcUrl,
                this.config.contractAddress,
                require('./abi.json')
            );
            this.logger.success('✅ 事件抓取器初始化成功');

            // 初始化數據驗證器
            this.logger.info('🔄 初始化數據驗證器...');
            this.dataValidator = new DataValidator(this.config.timezone);
            this.logger.success('✅ 數據驗證器初始化成功');

            // 初始化事務管理器
            this.logger.info('🔄 初始化事務管理器...');
            this.transactionManager = new TransactionManager(this.db);
            this.logger.success('✅ 事務管理器初始化成功');

            // 初始化調度器
            this.logger.info('🔄 初始化調度器...');
            this.scheduler = new Scheduler(this);
            this.logger.success('✅ 任務調度器初始化成功');

            // 獲取當前最新局次
            this.logger.info('🔄 獲取當前局次...');
            this.currentEpoch = await this.eventScraper.getCurrentEpoch();
            this.logger.startup(`當前最新局次：${this.currentEpoch}`);

        } catch (error) {
            console.error('❌ 初始化失敗:', error);
            console.error('❌ 錯誤堆棧:', error.stack);
            this.logger.error('❌ 系統初始化失敗:', error);
            throw error;
        }
    }

    /**
     * 啟動主線程 (持續歷史回溯)
     */
    async startMainThread() {
        this.logger.info('🔄 啟動主線程 (歷史數據回溯)');

        const processEpoch = async (epoch) => {
            await this.processEpoch(epoch);
        };

        await this.scheduler.startMainThread(processEpoch);

        // 設置定期重啟
        setInterval(() => {
            if (!this.isShuttingDown) {
                this.logger.info('🔄 主線程定期重啟');
                this.gracefulShutdown();
            }
        }, this.config.mainThreadRestartInterval);
    }

    /**
     * 啟動支線線程 (定期檢查最新局次)
     */
    async startSecondaryThread() {
        this.logger.info('🔄 啟動支線線程 (最新局次檢查)');

        const processEpochs = async () => {
            const targetEpochs = [
                this.currentEpoch - 2,
                this.currentEpoch - 3,
                this.currentEpoch - 4
            ].filter(epoch => epoch > 0);

            for (const epoch of targetEpochs) {
                await this.processEpoch(epoch);
            }
        };

        // 延遲首次執行
        setTimeout(async () => {
            await processEpochs();
            this.scheduler.startSecondaryThread(processEpochs, this.config.secondaryThreadInterval);
        }, this.config.initialSecondaryThreadDelay);
    }

    /**
     * 處理單個局次
     * @param {number} epoch 局次編號
     */
    async processEpoch(epoch) {
        this.logger.processing(epoch);

        try {
            // 1. 檢查 finEpoch 表
            const exists = await this.db.checkFinEpoch(epoch);
            if (exists) {
                this.logger.debug(`⏭️  局次 ${epoch} 已完成，跳過`);
                this.resetFailureCounter(); // 成功處理，重置失敗計數器
                return;
            }

            // 2. 嘗試獲取 Redis 鎖
            const lockAcquired = await this.redis.acquireLock(epoch.toString(), this.config.lockTimeout);
            if (!lockAcquired) {
                this.logger.debug(`🔒 局次 ${epoch} 正在被其他線程處理，跳過`);
                return;
            }

            this.logger.success(`🔓 成功獲取局次 ${epoch} 的鎖`);

            // 3. 執行完整的處理流程
            await this.handleEpochProcessing(epoch);

            // 4. 處理成功，重置失敗計數器
            this.resetFailureCounter();

        } catch (error) {
            console.error(`❌ 處理局次 ${epoch} 時發生錯誤:`);
            console.error(`❌ 錯誤對象:`, error);
            console.error(`❌ 錯誤類型:`, typeof error);
            console.error(`❌ 錯誤訊息:`, error?.message);
            console.error(`❌ 錯誤堆疊:`, error?.stack);
            console.error(`❌ 錯誤詳情:`, JSON.stringify(error, Object.getOwnPropertyNames(error)));

            // 如果 error 是空對象，檢查是否有其他信息
            if (Object.keys(error || {}).length === 0) {
                console.error(`❌ 空錯誤對象檢測 - 可能是事務管理器問題`);
            }

            // 🚨 記錄失敗並檢查是否需要中斷系統
            await this.handleProcessingFailure(epoch, error);

        } finally {
            // 5. 釋放鎖
            try {
                await this.redis.releaseLock(epoch.toString());
                this.logger.info(`🔓 釋放局次 ${epoch} 的鎖`);
            } catch (lockError) {
                this.logger.warn(`⚠️ 釋放鎖失敗: ${lockError.message}`);
            }
        }
    }

    /**
     * 執行完整的局次處理流程
     * @param {number} epoch 局次編號
     */
    async handleEpochProcessing(epoch) {
        // 3. 定位區塊範圍 (二分法)
        const blockRange = await this.eventScraper.getBlockRangeForEpoch(epoch);
        this.logger.info(`📍 局次 ${epoch} 區塊範圍: ${blockRange.from} - ${blockRange.to}`);

        // 4. 批量抓取事件
        const eventsData = await this.eventScraper.fetchEventsInRange(blockRange.from, blockRange.to);
        this.logger.blockchain('抓取事件', blockRange.to, Date.now());
        this.logger.info(`📊 抓取到 ${eventsData.totalEvents} 個事件`);

        // 5. 數據驗證
        const validationResult = await this.dataValidator.validateEpochData(eventsData);
        if (!validationResult.isValid) {
            throw new Error(`數據驗證失敗: ${validationResult.errors.join(', ')}`);
        }
        this.logger.success('✅ 數據驗證通過');

        // 6. 產生 multiClaim 資料
        const multiClaimData = this.generateMultiClaimData(validationResult.claimData);

        // 7. 執行事務性寫入
        await this.transactionManager.executeTransaction(async (trx) => {
            // 清理 realBet 臨時數據
            await trx.delete('realBet', { epoch: epoch });

            // 寫入歷史數據
            await trx.insert(validationResult.roundData, 'round');
            await trx.batchInsert(validationResult.hisBetData, 'hisBet');
            await trx.batchInsert(validationResult.claimData, 'claim');

            if (multiClaimData.length > 0) {
                await trx.batchInsert(multiClaimData, 'multiClaim');
            }

            // 標記完成
            await trx.insert({ epoch }, 'finEpoch');
        });

        this.logger.completed(epoch, Date.now());
    }

    /**
     * 生成 multiClaim 資料 (巨鯨行為偵測)
     * @param {Array} claimData claim 數據
     * @returns {Array} multiClaim 數據
     */
    generateMultiClaimData(claimData) {
        const walletClaims = {};

        // 按錢包地址聚合
        claimData.forEach(claim => {
            if (!walletClaims[claim.walletAddress]) {
                walletClaims[claim.walletAddress] = {
                    epoch: claim.epoch,
                    walletAddress: claim.walletAddress,
                    claimCount: 0,
                    totalAmount: 0
                };
            }

            walletClaims[claim.walletAddress].claimCount += 1;
            walletClaims[claim.walletAddress].totalAmount += parseFloat(claim.claimAmount);
        });

        // 過濾出符合條件的巨鯨行為
        return Object.values(walletClaims).filter(claim =>
            claim.claimCount >= 5 || claim.totalAmount >= 1
        );
    }

    /**
     * 記錄錯誤到 errEpoch 表
     * @param {number} epoch 局次編號
     * @param {string} errorMessage 錯誤訊息
     */
    async logError(epoch, errorMessage) {
        try {
            const errorData = {
                epoch: epoch,
                errorTime: moment().tz(this.config.timezone).format('YYYY-MM-DD HH:mm:ss'),
                errorMessage: errorMessage
            };

            await this.db.logError(errorData);
            this.logger.info(`📝 錯誤日誌已記錄 (局次 ${epoch})`);
        } catch (logError) {
            this.logger.error('❌ 記錄錯誤日誌失敗:', logError);
        }
    }

    /**
     * 🚨 處理處理失敗
     * @param {number} epoch 失敗的局次
     * @param {Error} error 錯誤對象
     */
    async handleProcessingFailure(epoch, error) {
        // 記錄錯誤到資料庫
        await this.logError(epoch, error?.message || JSON.stringify(error) || '未知錯誤');

        // 更新失敗計數器
        this.consecutiveFailures++;

        const now = Date.now();

        // 如果是第一次失敗或超出失敗窗口，重置窗口
        if (!this.failureWindowStart || (now - this.failureWindowStart) > this.failureWindowDuration) {
            this.failureWindowStart = now;
            this.consecutiveFailures = 1;
        }

        this.logger.error(`🚨 處理失敗計數: ${this.consecutiveFailures}/${this.maxConsecutiveFailures} (10分鐘窗口內)`);

        // 檢查是否達到中斷閾值
        if (this.consecutiveFailures >= this.maxConsecutiveFailures) {
            this.logger.error(`🚨 連續 ${this.maxConsecutiveFailures} 次處理失敗，系統將自動中斷！`);
            this.logger.error(`🚨 最後一次失敗: 局次 ${epoch}, 錯誤: ${error?.message || '未知錯誤'}`);

            // 強制中斷系統
            await this.forceShutdown(`連續 ${this.maxConsecutiveFailures} 次處理失敗`);
        }
    }

    /**
     * 重置失敗計數器
     */
    resetFailureCounter() {
        if (this.consecutiveFailures > 0) {
            this.logger.info(`✅ 處理成功，重置失敗計數器 (${this.consecutiveFailures} → 0)`);
            this.consecutiveFailures = 0;
            this.failureWindowStart = null;
        }
    }

    /**
     * 🚨 強制中斷系統
     * @param {string} reason 中斷原因
     */
    async forceShutdown(reason) {
        this.logger.error(`🚨 系統強制中斷: ${reason}`);

        if (this.isShuttingDown) return;
        this.isShuttingDown = true;

        try {
            // 停止所有定時任務
            if (this.scheduler) {
                await this.scheduler.stop();
            }

            // 關閉資料庫連接
            if (this.db) {
                await this.db.disconnect();
            }

            // 關閉 Redis 連接
            if (this.redis) {
                await this.redis.disconnect();
            }

            this.logger.error(`🚨 系統因 ${reason} 而中斷`);
            process.exit(1); // 使用退出碼 1 表示異常退出

        } catch (error) {
            console.error('❌ 強制關閉過程中發生錯誤:', error);
            process.exit(1);
        }
    }

    /**
     * 優雅關閉
     */
    async gracefulShutdown() {
        if (this.isShuttingDown) return;

        this.isShuttingDown = true;
        this.logger.shutdown('開始優雅關閉');

        try {
            // 停止所有定時任務
            if (this.scheduler) {
                await this.scheduler.stop();
            }

            // 關閉資料庫連接
            if (this.db) {
                await this.db.disconnect();
            }

            // 關閉 Redis 連接
            if (this.redis) {
                await this.redis.disconnect();
            }

            this.logger.shutdown('系統已安全關閉');
            process.exit(0);

        } catch (error) {
            this.logger.error('❌ 關閉過程中發生錯誤:', error);
            process.exit(1);
        }
    }

    /**
     * 啟動系統
     */
    async start() {
        try {
            await this.initialize();

            // 啟動主線和支線
            await Promise.all([
                this.startMainThread(),
                this.startSecondaryThread()
            ]);

            this.logger.info('🎉 HisBet 數據抓取系統已啟動並運行中...');

            // 優雅關閉處理
            process.on('SIGINT', () => this.gracefulShutdown());
            process.on('SIGTERM', () => this.gracefulShutdown());

        } catch (error) {
            this.logger.error('❌ 系統啟動失敗:', error);
            process.exit(1);
        }
    }
}

// 啟動應用
if (require.main === module) {
    const scraper = new HisBetScraper();
    scraper.start();
}

module.exports = HisBetScraper;