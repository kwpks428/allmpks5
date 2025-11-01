const { Pool } = require('pg');
const moment = require('moment-timezone');
const Logger = require('./logger');

/**
 * 資料庫管理器
 * 負責 PostgreSQL 連接、查詢和事務管理
 */
class Database {
    constructor(connectionString) {
        this.connectionString = connectionString;
        this.pool = null;
        this.logger = new Logger();
    }

    /**
     * 連接到資料庫
     */
    async connect() {
        try {
            this.pool = new Pool({
                connectionString: this.connectionString,
                max: 10, // 減少連接數
                min: 2,  // 最小連接數
                idleTimeoutMillis: 60000, // 增加空閒超時
                connectionTimeoutMillis: 10000, // 增加連接超時
                acquireTimeoutMillis: 60000, // 獲取連接超時
                query_timeout: 30000, // 查詢超時
                statement_timeout: 30000, // 語句超時
            });

            // 測試連接
            const client = await this.pool.connect();
            await client.query('SELECT NOW()');
            client.release();

            console.log('✅ 資料庫連接成功');

            this.pool.on('error', (err) => {
                console.error('❌ 資料庫連接錯誤:', err);
            });

        } catch (error) {
            console.error('❌ 資料庫連接失敗:', error);
            throw error;
        }
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
            // 查詢時間移到 debug 級別，避免輸出過多
            if (this.logger) {
                this.logger.debug(`📊 查詢執行時間: ${duration}ms, 查詢: ${text.substring(0, 50)}...`);
            }
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

    /**
     * 插入歷史局次數據
     * @param {Object} roundData 局次數據
     * @param {Object} client 可選的事務客戶端
     */
    async insertHistoryRound(roundData, client = null) {
        const db = client || this.pool;
        
        try {
            // 檢查是否已存在
            const existing = await db.query(
                'SELECT episode FROM history_rounds WHERE episode = $1',
                [roundData.episode]
            );

            if (existing.rows.length > 0) {
                // 更新現有記錄
                return await db.query(`
                    UPDATE history_rounds SET
                        start_block = $1,
                        start_timestamp = $2,
                        start_tx_hash = $3,
                        lock_block = $4,
                        lock_timestamp = $5,
                        lock_tx_hash = $6,
                        end_block = $7,
                        end_timestamp = $8,
                        end_tx_hash = $9,
                        episode_start_time = $10,
                        episode_lock_time = $11,
                        episode_end_time = $12,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE episode = $13
                `, [
                    roundData.startBlock,
                    roundData.startTimestamp,
                    roundData.startTxHash,
                    roundData.lockBlock || null,
                    roundData.lockTimestamp || null,
                    roundData.lockTxHash || null,
                    roundData.endBlock || null,
                    roundData.endTimestamp || null,
                    roundData.endTxHash || null,
                    new Date(Math.floor(roundData.startTimestamp * 1000)),
                    roundData.lockTimestamp ? new Date(Math.floor(roundData.lockTimestamp * 1000)) : null,
                    roundData.endTimestamp ? new Date(Math.floor(roundData.endTimestamp * 1000)) : null,
                    roundData.episode
                ]);
            } else {
                // 插入新記錄
                return await db.query(`
                    INSERT INTO history_rounds (
                        episode, start_block, start_timestamp, start_tx_hash,
                        lock_block, lock_timestamp, lock_tx_hash,
                        end_block, end_timestamp, end_tx_hash,
                        episode_start_time, episode_lock_time, episode_end_time,
                        created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                `, [
                    roundData.episode,
                    roundData.startBlock,
                    roundData.startTimestamp,
                    roundData.startTxHash,
                    roundData.lockBlock || null,
                    roundData.lockTimestamp || null,
                    roundData.lockTxHash || null,
                    roundData.endBlock || null,
                    roundData.endTimestamp || null,
                    roundData.endTxHash || null,
                    new Date(Math.floor(roundData.startTimestamp * 1000)),
                    roundData.lockTimestamp ? new Date(Math.floor(roundData.lockTimestamp * 1000)) : null,
                    roundData.endTimestamp ? new Date(Math.floor(roundData.endTimestamp * 1000)) : null
                ]);
            }
        } catch (error) {
            console.error(`❌ 插入歷史局次數據失敗 (episode: ${roundData.episode}):`, error);
            throw error;
        }
    }

    /**
     * 插入歷史投注數據
     * @param {Array} betData 投注數據陣列
     * @param {Object} client 可選的事務客戶端
     */
    async insertHistoryBet(betData, client = null) {
        if (!betData || betData.length === 0) return;
        
        const db = client || this.pool;
        
        try {
            for (const bet of betData) {
                await db.query(`
                    INSERT INTO history_bets (
                        epoch, user, amount, bet_amount, position,
                        bet_block, bet_timestamp, bet_tx_hash,
                        created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (epoch, user, bet_tx_hash) DO NOTHING
                `, [
                    bet.epoch,
                    bet.user,
                    bet.amount,
                    bet.amount,
                    bet.position,
                    bet.blockNumber,
                    bet.timestamp,
                    bet.transactionHash,
                ]);
            }
        } catch (error) {
            console.error(`❌ 插入歷史投注數據失敗:`, error);
            throw error;
        }
    }

    /**
     * 插入歷史認領數據
     * @param {Array} claimData 認領數據陣列
     * @param {Object} client 可選的事務客戶端
     */
    async insertHistoryClaim(claimData, client = null) {
        if (!claimData || claimData.length === 0) return;
        
        const db = client || this.pool;
        
        try {
            for (const claim of claimData) {
                await db.query(`
                    INSERT INTO history_claims (
                        epoch, user, amount, claim_amount,
                        claim_block, claim_timestamp, claim_tx_hash,
                        created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (epoch, user, claim_tx_hash) DO NOTHING
                `, [
                    claim.epoch,
                    claim.user,
                    claim.amount,
                    claim.amount,
                    claim.blockNumber,
                    claim.timestamp,
                    claim.transactionHash,
                ]);
            }
        } catch (error) {
            console.error(`❌ 插入歷史認領數據失敗:`, error);
            throw error;
        }
    }

    /**
     * 插入歷史多人認領數據
     * @param {Array} multiClaimData 多人認領數據陣列
     * @param {Object} client 可選的事務客戶端
     */
    async insertHistoryMultiClaim(multiClaimData, client = null) {
        if (!multiClaimData || multiClaimData.length === 0) return;
        
        const db = client || this.pool;
        
        try {
            for (const multiClaim of multiClaimData) {
                await db.query(`
                    INSERT INTO history_multi_claims (
                        epoch, users, amount, claim_amount,
                        claim_block, claim_timestamp, claim_tx_hash,
                        created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (epoch, claim_tx_hash) DO NOTHING
                `, [
                    multiClaim.epoch,
                    multiClaim.users,
                    multiClaim.amount,
                    multiClaim.amount,
                    multiClaim.blockNumber,
                    multiClaim.timestamp,
                    multiClaim.transactionHash,
                ]);
            }
        } catch (error) {
            console.error(`❌ 插入歷史多人認領數據失敗:`, error);
            throw error;
        }
    }

    /**
     * 插入歷史真實投注數據
     * @param {Array} realBetData 真實投注數據陣列
     * @param {Object} client 可選的事務客戶端
     */
    async insertHistoryRealBet(realBetData, client = null) {
        if (!realBetData || realBetData.length === 0) return;
        
        const db = client || this.pool;
        
        try {
            for (const realBet of realBetData) {
                await db.query(`
                    INSERT INTO history_real_bets (
                        epoch, user, amount, bet_amount, position,
                        bet_block, bet_timestamp, bet_tx_hash,
                        created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    ON CONFLICT (epoch, user, bet_tx_hash) DO NOTHING
                `, [
                    realBet.epoch,
                    realBet.user,
                    realBet.amount,
                    realBet.amount,
                    realBet.position,
                    realBet.blockNumber,
                    realBet.timestamp,
                    realBet.transactionHash,
                ]);
            }
        } catch (error) {
            console.error(`❌ 插入歷史真實投注數據失敗:`, error);
            throw error;
        }
    }
}

module.exports = Database;