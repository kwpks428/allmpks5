const { Pool } = require('pg');
const moment = require('moment-timezone');

/**
 * 資料庫管理器
 * 負責 PostgreSQL 連接、查詢和事務管理
 */
class Database {
    constructor(connectionString) {
        this.connectionString = connectionString;
        this.pool = null;
    }

    /**
     * 連接到資料庫
     */
    async connect() {
        this.pool = new Pool({
            connectionString: this.connectionString,
            max: 20,
            idleTimeoutMillis: 30000,
            connectionTimeoutMillis: 2000,
        });

        // 測試連接
        const client = await this.pool.connect();
        await client.query('SELECT NOW()');
        client.release();

        this.pool.on('error', (err) => {
            console.error('❌ 資料庫連接錯誤:', err);
        });
    }

    /**
     * 斷開資料庫連接
     */
    async disconnect() {
        if (this.pool) {
            await this.pool.end();
        }
    }

    /**
     * 執行查詢
     * @param {string} text SQL 查詢語句
     * @param {Array} params 參數
     * @returns {Promise} 查詢結果
     */
    async query(text, params) {
        const start = Date.now();
        try {
            const result = await this.pool.query(text, params);
            const duration = Date.now() - start;
            console.log(`📊 查詢執行時間: ${duration}ms, 查詢: ${text.substring(0, 50)}...`);
            return result;
        } catch (error) {
            console.error('❌ 資料庫查詢錯誤:', error);
            throw error;
        }
    }

    /**
     * 開始事務
     * @returns {Promise} 事務客戶端
     */
    async beginTransaction() {
        const client = await this.pool.connect();
        await client.query('BEGIN');
        return client;
    }

    /**
     * 提交事務
     * @param {Object} client 事務客戶端
     */
    async commitTransaction(client) {
        await client.query('COMMIT');
        client.release();
    }

    /**
     * 回滾事務
     * @param {Object} client 事務客戶端
     */
    async rollbackTransaction(client) {
        await client.query('ROLLBACK');
        client.release();
    }

    /**
     * 執行事務
     * @param {Function} callback 事務回調函數
     */
    async transaction(callback) {
        const client = await this.pool.connect();
        try {
            await client.query('BEGIN');
            const result = await callback(client);
            await client.query('COMMIT');
            return result;
        } catch (error) {
            await client.query('ROLLBACK');
            throw error;
        } finally {
            client.release();
        }
    }

    /**
     * 檢查 finEpoch 表中是否存在指定的 epoch
     * @param {number} epoch 局次編號
     * @returns {Promise<boolean>} 是否存在
     */
    async checkFinEpoch(epoch) {
        const result = await this.query(
            'SELECT 1 FROM finEpoch WHERE epoch = $1',
            [epoch]
        );
        return result.rows.length > 0;
    }

    /**
     * 記錄錯誤到 errEpoch 表
     * @param {Object} errorData 錯誤數據
     */
    async logError(errorData) {
        // 獨立於主事務，使用新連接
        const client = await this.pool.connect();
        try {
            await client.query(`
                INSERT INTO errEpoch (epoch, errorTime, errorMessage)
                VALUES ($1, $2, $3)
                ON CONFLICT (epoch) 
                DO UPDATE SET 
                    errorTime = EXCLUDED.errorTime,
                    errorMessage = EXCLUDED.errorMessage
            `, [errorData.epoch, errorData.errorTime, errorData.errorMessage]);
        } finally {
            client.release();
        }
    }

    /**
     * 獲取最新已完成處理的 epoch
     * @returns {Promise<number>} 最新 epoch
     */
    async getLatestProcessedEpoch() {
        const result = await this.query(
            'SELECT MAX(epoch) as maxEpoch FROM finEpoch'
        );
        return result.rows[0].maxepoch || 0;
    }

    /**
     * 批量插入數據
     * @param {string} tableName 表名
     * @param {Array} data 數據陣列
     * @param {Object} client 可選的事務客戶端
     */
    async batchInsert(tableName, data, client = null) {
        if (!data || data.length === 0) {
            return;
        }

        const db = client || this.pool;
        const table = this.sanitizeTableName(tableName);
        const columns = Object.keys(data[0]);
        const values = [];
        const placeholders = [];

        columns.forEach((col, index) => {
            values.push(data[0][col]);
            placeholders.push(`$${index + 1}`);
        });

        const query = `
            INSERT INTO ${table} (${columns.join(', ')})
            VALUES (${placeholders.join(', ')})
        `;

        const result = await db.query(query, values);
        return result;
    }

    /**
     * 清理表名（防止 SQL 注入）
     * @param {string} tableName 表名
     * @returns {string} 清理後的表名
     */
    sanitizeTableName(tableName) {
        const allowedTables = [
            'round', 'hisBet', 'claim', 'multiClaim', 
            'realBet', 'finEpoch', 'errEpoch'
        ];
        
        if (!allowedTables.includes(tableName)) {
            throw new Error(`不允許的表名: ${tableName}`);
        }
        
        return tableName;
    }

    /**
     * 獲取資料庫統計信息
     * @returns {Promise<Object>} 統計信息
     */
    async getStats() {
        const totalRounds = await this.query('SELECT COUNT(*) as count FROM round');
        const totalBets = await this.query('SELECT COUNT(*) as count FROM hisBet');
        const totalClaims = await this.query('SELECT COUNT(*) as count FROM claim');
        const processedEpochs = await this.query('SELECT COUNT(*) as count FROM finEpoch');
        const errorEpochs = await this.query('SELECT COUNT(*) as count FROM errEpoch');

        return {
            totalRounds: parseInt(totalRounds.rows[0].count),
            totalBets: parseInt(totalBets.rows[0].count),
            totalClaims: parseInt(totalClaims.rows[0].count),
            processedEpochs: parseInt(processedEpochs.rows[0].count),
            errorEpochs: parseInt(errorEpochs.rows[0].count)
        };
    }
}

module.exports = Database;