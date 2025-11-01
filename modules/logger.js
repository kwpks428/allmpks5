const moment = require('moment-timezone');
const fs = require('fs');
const path = require('path');

/**
 * 日誌系統
 * 提供統一的日誌記錄功能，支持多級別日誌和文件輸出
 */
class Logger {
    constructor(options = {}) {
        this.timezone = options.timezone || 'Asia/Taipei';
        this.logLevel = options.logLevel || 'info';
        this.enableFileLogging = options.enableFileLogging || true;
        this.maxLogSize = options.maxLogSize || 10 * 1024 * 1024; // 10MB
        this.maxFiles = options.maxFiles || 5;
        
        // 創建日誌目錄
        this.logDir = path.join(process.cwd(), 'logs');
        if (this.enableFileLogging && !fs.existsSync(this.logDir)) {
            fs.mkdirSync(this.logDir, { recursive: true });
        }
        
        // 日誌級別
        this.levels = {
            error: 0,
            warn: 1,
            info: 2,
            debug: 3
        };
        
        this.currentLogFile = null;
        this.today = moment().tz(this.timezone).format('YYYY-MM-DD');
        this.initializeLogFile();
        
        // 統計信息
        this.stats = {
            totalLogs: 0,
            errorLogs: 0,
            warnLogs: 0,
            infoLogs: 0,
            debugLogs: 0
        };
    }

    /**
     * 初始化日誌文件
     */
    initializeLogFile() {
        if (!this.enableFileLogging) return;
        
        const dateStr = moment().tz(this.timezone).format('YYYY-MM-DD');
        const logFileName = `hisbet-${dateStr}.log`;
        this.currentLogFile = path.join(this.logDir, logFileName);
        
        // 檢查文件大小，必要时輪換
        this.rotateLogIfNeeded();
    }

    /**
     * 檢查並輪換日誌文件
     */
    rotateLogIfNeeded() {
        if (!this.enableFileLogging || !fs.existsSync(this.currentLogFile)) return;
        
        try {
            const stats = fs.statSync(this.currentLogFile);
            if (stats.size > this.maxLogSize) {
                this.rotateLog();
            }
        } catch (error) {
            console.error('檢查日誌文件大小失敗:', error);
        }
    }

    /**
     * 輪換日誌文件
     */
    rotateLog() {
        if (!fs.existsSync(this.currentLogFile)) return;
        
        try {
            // 創建備份文件
            const timestamp = moment().tz(this.timezone).format('YYYY-MM-DD_HH-mm-ss');
            const backupFileName = `hisbet-${this.today}_${timestamp}.log`;
            const backupPath = path.join(this.logDir, backupFileName);
            
            fs.renameSync(this.currentLogFile, backupPath);
            
            // 清理舊文件
            this.cleanupOldLogs();
            
            // 更新當前文件
            this.initializeLogFile();
            
            console.log(`📝 日誌文件已輪換: ${backupFileName}`);
        } catch (error) {
            console.error('輪換日誌文件失敗:', error);
        }
    }

    /**
     * 清理舊的日誌文件
     */
    cleanupOldLogs() {
        try {
            const files = fs.readdirSync(this.logDir)
                .filter(file => file.startsWith('hisbet-') && file.endsWith('.log'))
                .map(file => ({
                    name: file,
                    path: path.join(this.logDir, file),
                    time: fs.statSync(path.join(this.logDir, file)).mtime
                }))
                .sort((a, b) => b.time - a.time);

            // 保留最新的文件
            files.slice(0, this.maxFiles).forEach(file => {
                if (file.time < moment().tz(this.timezone).subtract(7, 'days').toDate()) {
                    fs.unlinkSync(file.path);
                }
            });
        } catch (error) {
            console.error('清理舊日誌文件失敗:', error);
        }
    }

    /**
     * 格式化日誌消息
     * @param {string} level 日誌級別
     * @param {string} message 消息
     * @param {any} ...args 額外參數
     * @returns {string} 格式化後的消息
     */
    formatMessage(level, message, ...args) {
        const timestamp = moment().tz(this.timezone).format('YYYY-MM-DD HH:mm:ss');
        const levelUpper = level.toUpperCase();
        const processId = process.pid;
        const memUsage = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
        
        let formattedMessage = `[${timestamp}] [${levelUpper}] [PID:${processId}] [MEM:${memUsage}MB] ${message}`;
        
        if (args.length > 0) {
            formattedMessage += ' ' + args.map(arg => {
                if (typeof arg === 'object') {
                    return JSON.stringify(arg, null, 2);
                }
                return String(arg);
            }).join(' ');
        }
        
        return formattedMessage;
    }

    /**
     * 記錄日誌
     * @param {string} level 日誌級別
     * @param {string} message 消息
     * @param {any} ...args 額外參數
     */
    log(level, message, ...args) {
        // 檢查日誌級別
        if (this.levels[level] > this.levels[this.logLevel]) {
            return;
        }

        this.rotateLogIfNeeded();
        
        const formattedMessage = this.formatMessage(level, message, ...args);
        
        // 更新統計
        this.stats.totalLogs++;
        this.stats[`${level}Logs`]++;
        
        // 控制台輸出
        switch (level) {
            case 'error':
                console.error(formattedMessage);
                break;
            case 'warn':
                console.warn(formattedMessage);
                break;
            case 'info':
                console.info(formattedMessage);
                break;
            case 'debug':
                console.debug(formattedMessage);
                break;
            default:
                console.log(formattedMessage);
        }
        
        // 文件輸出
        if (this.enableFileLogging) {
            this.writeToFile(formattedMessage);
        }
    }

    /**
     * 寫入文件
     * @param {string} message 消息
     */
    writeToFile(message) {
        try {
            fs.appendFileSync(this.currentLogFile, message + '\n', 'utf8');
        } catch (error) {
            console.error('寫入日誌文件失敗:', error);
        }
    }

    /**
     * 錯誤日誌
     * @param {string} message 消息
     * @param {any} ...args 額外參數
     */
    error(message, ...args) {
        this.log('error', message, ...args);
    }

    /**
     * 警告日誌
     * @param {string} message 消息
     * @param {any} ...args 額外參數
     */
    warn(message, ...args) {
        this.log('warn', message, ...args);
    }

    /**
     * 信息日誌
     * @param {string} message 消息
     * @param {any} ...args 額外參數
     */
    info(message, ...args) {
        this.log('info', message, ...args);
    }

    /**
     * 調試日誌
     * @param {string} message 消息
     * @param {any} ...args 額外參數
     */
    debug(message, ...args) {
        this.log('debug', message, ...args);
    }

    /**
     * 記錄系統啟動
     */
    logStartup() {
        this.info('🚀 系統啟動', {
            nodeVersion: process.version,
            platform: process.platform,
            arch: process.arch,
            pid: process.pid,
            cwd: process.cwd(),
            logLevel: this.logLevel,
            timezone: this.timezone
        });
    }

    /**
     * 記錄系統關閉
     */
    logShutdown() {
        this.info('🔄 系統關閉', this.stats);
    }

    /**
     * 記錄性能統計
     * @param {string} operation 操作名稱
     * @param {number} duration 執行時間（毫秒）
     * @param {Object} metadata 額外元數據
     */
    performance(operation, duration, metadata = {}) {
        const memUsage = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
        this.info(`⚡ 性能統計 - ${operation}`, {
            duration: `${duration}ms`,
            memory: `${memUsage}MB`,
            ...metadata
        });
    }

    /**
     * 記錄數據庫操作
     * @param {string} operation 操作
     * @param {number} duration 執行時間
     * @param {any} result 結果
     */
    database(operation, duration, result) {
        this.debug(`💾 數據庫操作 - ${operation}`, {
            duration: `${duration}ms`,
            result: typeof result === 'object' ? `${result.rows?.length || 0} rows` : result
        });
    }

    /**
     * 記錄區塊鏈操作
     * @param {string} operation 操作
     * @param {number} blockNumber 區塊號
     * @param {number} duration 執行時間
     */
    blockchain(operation, blockNumber, duration) {
        this.info(`⛓️  區塊鏈操作 - ${operation}`, {
            block: blockNumber,
            duration: `${duration}ms`
        });
    }

    /**
     * 記錄錯誤堆疊
     * @param {Error} error 錯誤對象
     * @param {string} context 上下文
     */
    errorStack(error, context = '') {
        const stack = error.stack || error.toString();
        this.error(`❌ 錯誤堆疊 - ${context}`, {
            message: error.message,
            stack: stack.split('\n').slice(0, 10).join('\n') // 只保留前10行堆疊
        });
    }

    /**
     * 設置日誌級別
     * @param {string} level 新日誌級別
     */
    setLevel(level) {
        if (this.levels.hasOwnProperty(level)) {
            this.logLevel = level;
            this.info(`📝 日誌級別已設置為: ${level.toUpperCase()}`);
        } else {
            this.warn(`無效的日誌級別: ${level}`);
        }
    }

    /**
     * 獲取日誌統計
     * @returns {Object} 統計信息
     */
    getStats() {
        return {
            ...this.stats,
            currentLogFile: this.currentLogFile,
            logDir: this.logDir,
            logLevel: this.logLevel
        };
    }

    /**
     * 獲取最近的日誌
     * @param {number} lines 獲取行數
     * @returns {string[]} 日誌行數組
     */
    getRecentLogs(lines = 100) {
        if (!this.enableFileLogging || !fs.existsSync(this.currentLogFile)) {
            return [];
        }
        
        try {
            const content = fs.readFileSync(this.currentLogFile, 'utf8');
            const allLines = content.split('\n').filter(line => line.trim());
            return allLines.slice(-lines);
        } catch (error) {
            this.error('讀取日誌文件失敗:', error);
            return [];
        }
    }

    /**
     * 搜索日誌
     * @param {string} keyword 關鍵字
     * @param {string} level 級別過濾
     * @param {number} hours 查看時間範圍（小時）
     * @returns {string[]} 匹配的日誌
     */
    searchLogs(keyword, level = null, hours = 24) {
        const results = [];
        const startTime = moment().tz(this.timezone).subtract(hours, 'hours');
        
        try {
            const files = fs.readdirSync(this.logDir)
                .filter(file => file.startsWith('hisbet-') && file.endsWith('.log'))
                .map(file => path.join(this.logDir, file))
                .filter(filePath => {
                    const stats = fs.statSync(filePath);
                    return stats.mtime >= startTime.toDate();
                });

            for (const filePath of files) {
                const content = fs.readFileSync(filePath, 'utf8');
                const lines = content.split('\n');
                
                for (const line of lines) {
                    if (line.includes(keyword)) {
                        if (!level || line.includes(`[${level.toUpperCase()}]`)) {
                            results.push(line);
                        }
                    }
                }
            }
        } catch (error) {
            this.error('搜索日誌失敗:', error);
        }
        
        return results;
    }

    /**
     * 清理資源
     */
    cleanup() {
        this.logShutdown();
    }
}

// 全域日誌實例
let globalLogger = null;

/**
 * 獲取全域日誌實例
 * @param {Object} options 選項
 * @returns {Logger} 日誌實例
 */
function getLogger(options = {}) {
    if (!globalLogger) {
        globalLogger = new Logger(options);
        globalLogger.logStartup();
    }
    return globalLogger;
}

/**
 * 設置全域日誌實例
 * @param {Logger} logger 日誌實例
 */
function setLogger(logger) {
    if (globalLogger) {
        globalLogger.cleanup();
    }
    globalLogger = logger;
}

// 導出
module.exports = Logger;
module.exports.getLogger = getLogger;
module.exports.setLogger = setLogger;