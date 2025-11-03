/**
 * 事務管理器
 * 負責數據庫事務的執行和管理，確保數據一致性
 */
class TransactionManager {
    constructor(database) {
        this.database = database;
    }

    /**
     * 執行事務
     * @param {Function} transactionCallback 事務回調函數
     * @returns {Promise} 事務結果
     */
    async executeTransaction(transactionCallback) {
        const client = await this.database.beginTransaction();
        
        try {
            console.log('📝 開始執行數據庫事務...');
            
            // 為客戶端提供便利方法
            const trx = this.createTransactionClient(client);
            
            // 執行事務邏輯
            const result = await transactionCallback(trx);
            
            // 提交事務
            await this.database.commitTransaction(client);
            console.log('✅ 數據庫事務提交成功');
            
            return result;
            
        } catch (error) {
            // 回滾事務
            await this.database.rollbackTransaction(client);
            console.error('❌ 數據庫事務回滾:', error.message);
            throw error;
        }
    }

    /**
     * 創建事務客戶端包裝器
     * @param {Object} client 原生資料庫客戶端
     * @returns {Object} 事務客戶端
     */
    createTransactionClient(client) {
        return {
            /**
             * 執行查詢
             * @param {string} text SQL 查詢
             * @param {Array} params 參數
             * @returns {Promise} 查詢結果
             */
            query: (text, params) => client.query(text, params),
            
            /**
             * 插入數據
             * @param {Object} data 數據對象
             * @param {string} tableName 表名
             * @returns {Promise} 插入結果
             */
            insert: (data, tableName) => {
                const sanitizedTable = this.database.sanitizeTableName(tableName);
                const columns = Object.keys(data);
                const values = Object.values(data);
                const placeholders = columns.map((_, index) => `$${index + 1}`).join(', ');
                
                const qcols = columns.map(c => '"' + c + '"').join(', ');
                const query = `INSERT INTO ${sanitizedTable} (${qcols}) VALUES (${placeholders})`;
                return client.query(query, values);
            },
            
            /**
             * 批量插入
             * @param {Array} dataArray 數據陣列
             * @param {string} tableName 表名
             * @returns {Promise} 插入結果
             */
            batchInsert: (dataArray, tableName) => {
                if (!dataArray || dataArray.length === 0) {
                    return Promise.resolve({ rowCount: 0 });
                }

                const sanitizedTable = this.database.sanitizeTableName(tableName);
                const columns = Object.keys(dataArray[0]);
                const qcols = columns.map(c => '"' + c + '"').join(', ');
                const values = [];

                const rowPlaceholders = dataArray.map((data, rowIndex) => {
                    const ph = columns.map((col, colIndex) => {
                        const paramIndex = rowIndex * columns.length + colIndex + 1;
                        values.push(data[col]);
                        return `${paramIndex}`;
                    });
                    return `(${ph.join(', ')})`;
                }).join(', ');

                const query = `
                    INSERT INTO ${sanitizedTable} (${qcols})
                    VALUES ${rowPlaceholders}
                `;

                return client.query(query, values);
            },
            
            /**
             * 刪除數據
             * @param {string} tableName 表名
             * @param {Object} conditions 條件
             * @returns {Promise} 刪除結果
             */
            delete: (tableName, conditions) => {
                const sanitizedTable = this.database.sanitizeTableName(tableName);
                const whereClause = this.buildWhereClause(conditions);
                const query = `DELETE FROM ${sanitizedTable} ${whereClause.sql}`;
                return client.query(query, whereClause.params);
            },
            
            /**
             * 更新數據
             * @param {string} tableName 表名
             * @param {Object} data 更新數據
             * @param {Object} conditions 條件
             * @returns {Promise} 更新結果
             */
            update: (tableName, data, conditions) => {
                const sanitizedTable = this.database.sanitizeTableName(tableName);
                const setClause = this.buildSetClause(data);
                const whereClause = this.buildWhereClause(conditions, setClause.params.length);
                
                const query = `
                    UPDATE ${sanitizedTable} 
                    SET ${setClause.sql} 
                    ${whereClause.sql}
                `;
                
                return client.query(query, [...setClause.params, ...whereClause.params]);
            },
            
            /**
             * 查詢數據
             * @param {string} tableName 表名
             * @param {Object} conditions 條件
             * @param {Array} columns 查詢列
             * @returns {Promise} 查詢結果
             */
            select: (tableName, conditions = {}, columns = ['*']) => {
                const sanitizedTable = this.database.sanitizeTableName(tableName);
                const whereClause = this.buildWhereClause(conditions);
                const columnsStr = Array.isArray(columns) ? columns.map(c => c === '*' ? '*' : '"' + c + '"').join(', ') : columns;
                
                const query = `SELECT ${columnsStr} FROM ${sanitizedTable} ${whereClause.sql}`;
                return client.query(query, whereClause.params);
            },
            
            /**
             * 批量操作支持 - 簡化版本
             * @param {string} action 操作類型
             * @param {Object} options 選項
             * @returns {Promise} 操作結果
             */
            raw: (action, options) => {
                // 提供原生客戶端訪問用於複雜查詢
                return client.query(options.sql, options.params);
            }
        };
    }

    /**
     * 建構 WHERE 子句
     * @param {Object} conditions 條件
     * @returns {Object} WHERE 子句和參數
     */
    buildWhereClause(conditions, offset = 0) {
        if (!conditions || Object.keys(conditions).length === 0) {
            return { sql: '', params: [] };
        }

        const params = [];
        const parts = [];
        let paramIndex = offset;

        for (const [key, value] of Object.entries(conditions)) {
            const qkey = '"' + key + '"';
            if (Array.isArray(value)) {
                const ph = value.map((v) => {
                    paramIndex += 1;
                    params.push(v);
                    return `${paramIndex}`;
                }).join(', ');
                parts.push(`${qkey} IN (${ph})`);
            } else if (value === null) {
                parts.push(`${qkey} IS NULL`);
            } else {
                paramIndex += 1;
                params.push(value);
                parts.push(`${qkey} = ${paramIndex}`);
            }
        }

        const sql = parts.length ? `WHERE ${parts.join(' AND ')}` : '';
        return { sql, params };
    }

    /**
     * 建構 SET 子句
     * @param {Object} data 更新數據
     * @returns {Object} SET 子句和參數
     */
    buildSetClause(data) {
        const keys = Object.keys(data);
        const setArray = keys.map((key, index) => `"${key}" = ${index + 1}`);
        const sql = setArray.join(', ');
        const params = keys.map(k => data[k]);
        return { sql, params };
    }

    /**
     * 執行批量事務操作
     * @param {Array} operations 操作陣列
     * @returns {Promise<Array>} 結果陣列
     */
    async executeBatchTransaction(operations) {
        const client = await this.database.beginTransaction();
        const results = [];

        try {
            const trx = this.createTransactionClient(client);
            
            for (let i = 0; i < operations.length; i++) {
                const operation = operations[i];
                console.log(`📝 執行批量操作 ${i + 1}/${operations.length}: ${operation.type}`);
                
                let result;
                switch (operation.type) {
                    case 'insert':
                        result = await trx.insert(operation.data, operation.table);
                        break;
                    case 'batchInsert':
                        result = await trx.batchInsert(operation.data, operation.table);
                        break;
                    case 'delete':
                        result = await trx.delete(operation.table, operation.conditions);
                        break;
                    case 'update':
                        result = await trx.update(operation.table, operation.data, operation.conditions);
                        break;
                    case 'select':
                        result = await trx.select(operation.table, operation.conditions, operation.columns);
                        break;
                    default:
                        throw new Error(`不支持的操作類型: ${operation.type}`);
                }
                
                results.push(result);
            }

            await this.database.commitTransaction(client);
            console.log(`✅ 批量事務執行成功，共 ${operations.length} 個操作`);
            
            return results;

        } catch (error) {
            await this.database.rollbackTransaction(client);
            console.error('❌ 批量事務回滾:', error.message);
            throw error;
        }
    }

    /**
     * 檢查表是否存在
     * @param {string} tableName 表名
     * @returns {Promise<boolean>} 表是否存在
     */
    async tableExists(tableName) {
        try {
            const sanitizedTable = this.database.sanitizeTableName(tableName);
            const result = await this.database.query(`
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = $1
                )
            `, [sanitizedTable]);
            
            return result.rows[0].exists;
        } catch (error) {
            console.error(`❌ 檢查表 ${tableName} 是否存在失敗:`, error);
            return false;
        }
    }

    /**
     * 獲取表結構
     * @param {string} tableName 表名
     * @returns {Promise<Array>} 表結構
     */
    async getTableSchema(tableName) {
        try {
            const sanitizedTable = this.database.sanitizeTableName(tableName);
            const result = await this.database.query(`
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = $1
                ORDER BY ordinal_position
            `, [sanitizedTable]);
            
            return result.rows;
        } catch (error) {
            console.error(`❌ 獲取表 ${tableName} 結構失敗:`, error);
            return [];
        }
    }

    /**
     * 事務監控和統計
     * @returns {Promise<Object>} 統計信息
     */
    async getTransactionStats() {
        try {
            // 獲取當前活動事務
            const activeTransactions = await this.database.query(`
                SELECT pid, usename, application_name, state, query, query_start, state_change
                FROM pg_stat_activity 
                WHERE state = 'active' 
                AND query LIKE '%BEGIN%' 
                OR query LIKE '%START TRANSACTION%'
            `);

            // 獲取事務統計
            const transactionStats = await this.database.query(`
                SELECT 
                    xact_commit,
                    xact_rollback,
                    xact_commit + xact_rollback as xact_total
                FROM pg_stat_database 
                WHERE datname = current_database()
            `);

            return {
                activeTransactions: activeTransactions.rows.length,
                totalCommits: parseInt(transactionStats.rows[0]?.xact_commit || 0),
                totalRollbacks: parseInt(transactionStats.rows[0]?.xact_rollback || 0),
                totalTransactions: parseInt(transactionStats.rows[0]?.xact_total || 0),
                rollbackRate: transactionStats.rows[0]?.xact_total > 0 
                    ? (parseInt(transactionStats.rows[0]?.xact_rollback || 0) / parseInt(transactionStats.rows[0]?.xact_total || 1) * 100).toFixed(2)
                    : '0.00'
            };
        } catch (error) {
            console.error('❌ 獲取事務統計失敗:', error);
            return {
                activeTransactions: 0,
                totalCommits: 0,
                totalRollbacks: 0,
                totalTransactions: 0,
                rollbackRate: '0.00'
            };
        }
    }
}

module.exports = TransactionManager;