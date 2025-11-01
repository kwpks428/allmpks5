/**
 * 任務調度器
 * 負責管理主線和支線任務的執行計劃
 */
class Scheduler {
    constructor(scraperInstance) {
        this.scraper = scraperInstance;
        this.mainThreadInterval = null;
        this.secondaryThreadInterval = null;
        this.isRunning = false;
        this.processedEpochs = new Set();
        this.failedEpochs = new Map();
        this.stats = {
            mainThread: {
                processed: 0,
                skipped: 0,
                failed: 0,
                lastRun: null
            },
            secondaryThread: {
                processed: 0,
                skipped: 0,
                failed: 0,
                lastRun: null
            }
        };
    }

    /**
     * 啟動主線程任務調度
     * @param {Function} processEpoch 處理局次的函數
     */
    async startMainThread(processEpoch) {
        if (this.mainThreadInterval) {
            console.log('⚠️  主線程已在運行中');
            return;
        }

        this.isRunning = true;
        console.log('🔄 啟動主線程任務調度...');

        // 主線程邏輯：從 currentEpoch-2 開始向歷史回溯
        const mainThreadLoop = async () => {
            if (!this.isRunning) return;

            try {
                console.log('🔄 主線程執行循環...');
                this.stats.mainThread.lastRun = new Date();

                // 獲取當前最新局次
                const currentEpoch = await this.scraper.eventScraper.getCurrentEpoch();
                const startEpoch = currentEpoch - 2;

                // 從最新的未處理局次開始向歷史回溯
                let targetEpoch = startEpoch;
                let processedInThisCycle = 0;

                // 處理多個局次直到達到處理限制
                while (targetEpoch > 0 && processedInThisCycle < 10) { // 每次循環最多處理10個局次
                    if (this.processedEpochs.has(targetEpoch)) {
                        console.log(`⏭️  局次 ${targetEpoch} 已處理過，跳過`);
                        targetEpoch--;
                        continue;
                    }

                    try {
                        console.log(`🎯 主線程處理局次: ${targetEpoch}`);
                        await processEpoch(targetEpoch);
                        
                        this.processedEpochs.add(targetEpoch);
                        this.stats.mainThread.processed++;
                        processedInThisCycle++;
                        
                        // 短暂暂停以避免过载
                        await this.sleep(1000);
                        
                    } catch (error) {
                        console.error(`❌ 主線程處理局次 ${targetEpoch} 失敗:`, error);
                        this.stats.mainThread.failed++;
                        this.failedEpochs.set(targetEpoch, {
                            error: error.message,
                            timestamp: new Date(),
                            thread: 'main'
                        });
                        
                        // 失敗的局次暫時跳過，後續可能會重試
                        targetEpoch--;
                    }

                    targetEpoch--;
                }

                console.log(`✅ 主線程循環完成，本次處理 ${processedInThisCycle} 個局次`);

            } catch (error) {
                console.error('❌ 主線程循環錯誤:', error);
                this.stats.mainThread.failed++;
            }

            // 設置下次執行
            if (this.isRunning) {
                // 主線程連續運行，但每次執行間有短暫休息
                setTimeout(mainThreadLoop, 5000); // 5秒後再次執行
            }
        };

        // 立即開始第一次執行
        setTimeout(mainThreadLoop, 1000);
    }

    /**
     * 啟動支線線程任務調度
     * @param {Function} processEpochs 處理局次的函數
     * @param {number} interval 執行間隔（毫秒）
     */
    async startSecondaryThread(processEpochs, interval = 5 * 60 * 1000) {
        if (this.secondaryThreadInterval) {
            console.log('⚠️  支線線程已在運行中');
            return;
        }

        this.isRunning = true;
        console.log(`🔄 啟動支線線程任務調度，間隔: ${interval / 1000}秒...`);

        const secondaryThreadLoop = async () => {
            if (!this.isRunning) return;

            try {
                console.log('🔄 支線線程執行循環...');
                this.stats.secondaryThread.lastRun = new Date();

                await processEpochs();

            } catch (error) {
                console.error('❌ 支線線程循環錯誤:', error);
                this.stats.secondaryThread.failed++;
            }

            // 設置下次執行
            if (this.isRunning) {
                this.secondaryThreadInterval = setTimeout(secondaryThreadLoop, interval);
            }
        };

        this.secondaryThreadInterval = setTimeout(secondaryThreadLoop, interval);
    }

    /**
     * 停止所有調度任務
     */
    async stop() {
        console.log('🛑 停止任務調度器...');
        this.isRunning = false;

        // 清除主線程間隔器
        if (this.mainThreadInterval) {
            clearTimeout(this.mainThreadInterval);
            this.mainThreadInterval = null;
        }

        // 清除支線線程間隔器
        if (this.secondaryThreadInterval) {
            clearTimeout(this.secondaryThreadInterval);
            this.secondaryThreadInterval = null;
        }

        console.log('✅ 任務調度器已停止');
    }

    /**
     * 獲取調度器狀態
     * @returns {Object} 狀態信息
     */
    getStatus() {
        return {
            isRunning: this.isRunning,
            processedEpochs: this.processedEpochs.size,
            failedEpochs: this.failedEpochs.size,
            stats: this.stats,
            mainThread: {
                running: this.mainThreadInterval !== null,
                interval: 'continuous'
            },
            secondaryThread: {
                running: this.secondaryThreadInterval !== null,
                interval: this.secondaryThreadInterval ? 'active' : 'inactive'
            }
        };
    }

    /**
     * 手動觸發主線程執行
     * @param {Function} processEpoch 處理函數
     */
    async triggerMainThread(processEpoch) {
        if (!this.isRunning) {
            console.log('⚠️  調度器未運行，無法觸發主線程');
            return;
        }

        console.log('🔄 手動觸發主線程執行...');
        try {
            await processEpoch();
            this.stats.mainThread.processed++;
        } catch (error) {
            console.error('❌ 手動觸發主線程失敗:', error);
            this.stats.mainThread.failed++;
        }
    }

    /**
     * 手動觸發支線線程執行
     * @param {Function} processEpochs 處理函數
     */
    async triggerSecondaryThread(processEpochs) {
        if (!this.isRunning) {
            console.log('⚠️  調度器未運行，無法觸發支線線程');
            return;
        }

        console.log('🔄 手動觸發支線線程執行...');
        try {
            await processEpochs();
            this.stats.secondaryThread.processed++;
        } catch (error) {
            console.error('❌ 手動觸發支線線程失敗:', error);
            this.stats.secondaryThread.failed++;
        }
    }

    /**
     * 重置統計信息
     */
    resetStats() {
        console.log('🔄 重置調度器統計信息');
        this.stats = {
            mainThread: {
                processed: 0,
                skipped: 0,
                failed: 0,
                lastRun: null
            },
            secondaryThread: {
                processed: 0,
                skipped: 0,
                failed: 0,
                lastRun: null
            }
        };
        this.processedEpochs.clear();
        this.failedEpochs.clear();
    }

    /**
     * 獲取失敗的局次
     * @returns {Array} 失敗局次列表
     */
    getFailedEpochs() {
        return Array.from(this.failedEpochs.entries()).map(([epoch, info]) => ({
            epoch,
            ...info
        }));
    }

    /**
     * 清理已解決的失敗局次
     */
    cleanupFailedEpochs() {
        console.log('🧹 清理已解決的失敗局次');
        const beforeCount = this.failedEpochs.size;
        this.failedEpochs.clear();
        console.log(`✅ 清理完成，從 ${beforeCount} 個失敗局次`);
    }

    /**
     * 獲取性能統計
     * @returns {Object} 性能統計
     */
    getPerformanceStats() {
        const now = new Date();
        const mainThreadUptime = this.stats.mainThread.lastRun 
            ? (now - this.stats.mainThread.lastRun) / 1000 
            : 0;
        const secondaryThreadUptime = this.stats.secondaryThread.lastRun 
            ? (now - this.stats.secondaryThread.lastRun) / 1000 
            : 0;

        return {
            uptime: now,
            totalProcessed: this.stats.mainThread.processed + this.stats.secondaryThread.processed,
            totalFailed: this.stats.mainThread.failed + this.stats.secondaryThread.failed,
            successRate: (() => {
                const total = this.stats.mainThread.processed + this.stats.mainThread.failed + 
                            this.stats.secondaryThread.processed + this.stats.secondaryThread.failed;
                return total > 0 ? 
                    ((this.stats.mainThread.processed + this.stats.secondaryThread.processed) / total * 100).toFixed(2) 
                    : '0.00';
            })(),
            avgProcessingTime: {
                mainThread: this.stats.mainThread.processed > 0 ? 
                    (mainThreadUptime / this.stats.mainThread.processed).toFixed(2) : '0.00',
                secondaryThread: this.stats.secondaryThread.processed > 0 ? 
                    (secondaryThreadUptime / this.stats.secondaryThread.processed).toFixed(2) : '0.00'
            },
            currentLoad: {
                processedEpochs: this.processedEpochs.size,
                failedEpochs: this.failedEpochs.size,
                pendingEpochs: this.getPendingEpochs().length
            }
        };
    }

    /**
     * 獲取待處理的局次
     * @returns {Array} 待處理局次列表
     */
    getPendingEpochs() {
        // 這裡實現待處理局次的邏輯
        // 實際實現中可能需要從資料庫查詢
        return [];
    }

    /**
     * 延遲函數
     * @param {number} ms 毫秒
     * @returns {Promise} Promise
     */
    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    /**
     * 監控系統資源使用情況
     * @returns {Object} 資源使用情況
     */
    getResourceUsage() {
        const memUsage = process.memoryUsage();
        const cpuUsage = process.cpuUsage();

        return {
            memory: {
                rss: Math.round(memUsage.rss / 1024 / 1024 * 100) / 100, // MB
                heapTotal: Math.round(memUsage.heapTotal / 1024 / 1024 * 100) / 100, // MB
                heapUsed: Math.round(memUsage.heapUsed / 1024 / 1024 * 100) / 100, // MB
                external: Math.round(memUsage.external / 1024 / 1024 * 100) / 100, // MB
            },
            cpu: {
                user: cpuUsage.user,
                system: cpuUsage.system
            },
            uptime: Math.round(process.uptime()),
            scheduler: {
                isRunning: this.isRunning,
                threads: {
                    main: this.mainThreadInterval ? 'active' : 'inactive',
                    secondary: this.secondaryThreadInterval ? 'active' : 'inactive'
                }
            }
        };
    }
}

module.exports = Scheduler;