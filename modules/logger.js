// modules/logger.js - 優化版
const moment = require('moment-timezone');
const fs = require('fs');
const path = require('path');

/**
 * 優化版日誌系統 - 支援顏色和重要性分級
 */
class Logger {
    constructor(options = {}) {
        this.timezone = options.timezone || 'Asia/Taipei';
        this.logLevel = options.logLevel || 'info';
        this.enableFileLogging = options.enableFileLogging ?? true;
        this.enableConsoleColors = options.enableConsoleColors ?? true;
        this.maxLogSize = options.maxLogSize || 10 * 1024 * 1024;
        this.maxFiles = options.maxFiles || 5;
        
        // 🎨 顏色定義 (ANSI 顏色碼)
        this.colors = {
            reset: '\x1b[0m',
            bright: '\x1b[1m',
            red: '\x1b[31m',
            green: '\x1b[32m',
            yellow: '\x1b[33m',
            blue: '\x1b[34m',
            magenta: '\x1b[35m',
            cyan: '\x1b[36m',
            white: '\x1b[37m',
            gray: '\x1b[90m'
        };

        // 🎯 日誌級別配置
        this.levels = {
            error: { value: 0, color: 'red', icon: '❌', prefix: 'ERROR' },
            warn: { value: 1, color: 'yellow', icon: '⚠️', prefix: 'WARN' },
            info: { value: 2, color: 'blue', icon: 'ℹ️', prefix: 'INFO' },
            success: { value: 2, color: 'green', icon: '✅', prefix: 'SUCCESS' },
            debug: { value: 3, color: 'gray', icon: '🔍', prefix: 'DEBUG' },
            performance: { value: 2, color: 'magenta', icon: '⚡', prefix: 'PERF' },
            database: { value: 3, color: 'cyan', icon: '💾', prefix: 'DB' },
            blockchain: { value: 2, color: 'yellow', icon: '⛓️', prefix: 'CHAIN' }
        };
        
        this.initializeLogFile();
        this.stats = {
            totalLogs: 0,
            errorLogs: 0,
            warnLogs: 0,
            infoLogs: 0,
            debugLogs: 0
        };
    }

    /**
     * 🎨 添加顏色到文本
     */
    colorize(text, color) {
        if (!this.enableConsoleColors) return text;
        return `${this.colors[color]}${text}${this.colors.reset}`;
    }

    /**
     * 📝 格式化日誌消息 - 優化版
     */
    formatMessage(level, message, ...args) {
        const timestamp = moment().tz(this.timezone).format('HH:mm:ss');
        const levelConfig = this.levels[level] || this.levels.info;
        const processId = process.pid;
        const memUsage = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
        
        // 🎯 控制台版本 (帶顏色)
        const consoleMessage = this.enableConsoleColors ? 
            `${this.colorize(levelConfig.icon, levelConfig.color)} ${this.colorize(`[${timestamp}]`, 'gray')} ${this.colorize(`[${memUsage}MB]`, 'gray')} ${message}` :
            `${levelConfig.icon} [${timestamp}] [${memUsage}MB] ${message}`;
            
        // 🗃️ 文件版本 (無顏色)
        const fileMessage = `[${moment().tz(this.timezone).format('YYYY-MM-DD HH:mm:ss')}] [${levelConfig.prefix}] [PID:${processId}] [MEM:${memUsage}MB] ${message}`;
        
        return { consoleMessage, fileMessage };
    }

    /**
     * 📊 記錄日誌 - 優化版
     */
    log(level, message, ...args) {
        const levelConfig = this.levels[level];
        if (!levelConfig || levelConfig.value > this.levels[this.logLevel].value) {
            return;
        }

        const { consoleMessage, fileMessage } = this.formatMessage(level, message, ...args);
        
        // 更新統計
        this.stats.totalLogs++;
        if (this.stats[`${level}Logs`] !== undefined) {
            this.stats[`${level}Logs`]++;
        }
        
        // 🖥️ 控制台輸出 (帶顏色)
        console.log(consoleMessage);
        
        // 🗃️ 文件輸出 (無顏色)
        if (this.enableFileLogging) {
            this.writeToFile(fileMessage);
        }
    }

    // 🎯 優化的快捷方法
    error(message, ...args) { this.log('error', message, ...args); }
    warn(message, ...args) { this.log('warn', message, ...args); }
    info(message, ...args) { this.log('info', message, ...args); }
    success(message, ...args) { this.log('success', message, ...args); }
    debug(message, ...args) { this.log('debug', message, ...args); }

    // 🚀 特殊類型日誌
    startup(message) { this.log('success', `🚀 ${message}`); }
    shutdown(message) { this.log('info', `🔄 ${message}`); }
    processing(epoch) { this.log('info', `🎯 處理局次: ${epoch}`); }
    completed(epoch, duration) { this.log('success', `✅ 局次 ${epoch} 處理完成 (${duration}ms)`); }
    failed(epoch, error) { this.log('error', `❌ 局次 ${epoch} 處理失敗: ${error}`); }
    
    performance(operation, duration, metadata = {}) {
        const memUsage = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
        this.log('performance', `${operation} (${duration}ms, ${memUsage}MB)`, metadata);
    }
    
    database(operation, duration, result) {
        if (this.logLevel === 'debug') {
            this.log('database', `${operation} (${duration}ms)`, result);
        }
    }
    
    blockchain(operation, blockNumber, duration) {
        this.log('blockchain', `${operation} 區塊:${blockNumber} (${duration}ms)`);
    }

    // 🧹 保留原有功能的方法
    initializeLogFile() {
        if (!this.enableFileLogging) return;
        
        this.logDir = path.join(process.cwd(), 'logs');
        if (!fs.existsSync(this.logDir)) {
            fs.mkdirSync(this.logDir, { recursive: true });
        }
        
        const dateStr = moment().tz(this.timezone).format('YYYY-MM-DD');
        const logFileName = `hisbet-${dateStr}.log`;
        this.currentLogFile = path.join(this.logDir, logFileName);
    }

    writeToFile(message) {
        try {
            fs.appendFileSync(this.currentLogFile, message + '\n', 'utf8');
        } catch (error) {
            console.error('寫入日誌文件失敗:', error);
        }
    }

    // 保留原有的其他方法...
    logStartup() {
        this.startup('系統啟動', {
            nodeVersion: process.version,
            platform: process.platform,
            arch: process.arch,
            pid: process.pid,
            cwd: process.cwd(),
            logLevel: this.logLevel,
            timezone: this.timezone
        });
    }

    logShutdown() {
        this.shutdown('系統關閉', this.stats);
    }

    setLevel(level) {
        if (this.levels.hasOwnProperty(level)) {
            this.logLevel = level;
            this.info(`📝 日誌級別已設置為: ${level.toUpperCase()}`);
        } else {
            this.warn(`無效的日誌級別: ${level}`);
        }
    }

    getStats() {
        return {
            ...this.stats,
            currentLogFile: this.currentLogFile,
            logDir: this.logDir,
            logLevel: this.logLevel
        };
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

module.exports = Logger;
module.exports.getLogger = getLogger;
module.exports.setLogger = setLogger;