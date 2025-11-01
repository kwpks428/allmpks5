const { createClient } = require('redis');

/**
 * Redis 鎖管理器
 * 實現分佈式鎖機制，防止主線和支線之間的 race condition
 */
class RedisLock {
    constructor(redisUrl) {
        this.redisUrl = redisUrl;
        this.client = null;
        this.lockPrefix = 'lock:pancake:epoch:';
    }

    /**
     * 連接到 Redis
     */
    async connect() {
        try {
            this.client = createClient({
                url: this.redisUrl
            });

            this.client.on('error', (err) => {
                console.error('❌ Redis 客戶端錯誤:', err);
            });

            await this.client.connect();
            console.log('✅ Redis 連接成功');
        } catch (error) {
            console.error('❌ Redis 連接失敗:', error);
            throw error;
        }
    }

    /**
     * 斷開 Redis 連接
     */
    async disconnect() {
        if (this.client) {
            await this.client.disconnect();
        }
    }

    /**
     * 嘗試獲取鎖
     * @param {string} key 鎖鍵
     * @param {number} ttl 過期時間（秒）
     * @returns {Promise<boolean>} 是否獲取成功
     */
    async acquireLock(key, ttl = 120) {
        try {
            const lockKey = key.startsWith(this.lockPrefix) ? key : this.lockPrefix + key;
            const result = await this.client.set(lockKey, 'processing', {
                NX: true, // Not Exists
                EX: ttl  // Expire
            });

            const success = result === 'OK';
            if (success) {
                console.log(`🔓 成功獲取鎖: ${lockKey} (TTL: ${ttl}s)`);
            } else {
                console.log(`🔒 鎖已被佔用: ${lockKey}`);
            }
            
            return success;
        } catch (error) {
            console.error('❌ 獲取鎖失敗:', error);
            throw error;
        }
    }

    /**
     * 釋放鎖
     * @param {string} key 鎖鍵
     * @returns {Promise<boolean>} 是否釋放成功
     */
    async releaseLock(key) {
        try {
            const lockKey = key.startsWith(this.lockPrefix) ? key : this.lockPrefix + key;
            const result = await this.client.del(lockKey);

            if (result > 0) {
                console.log(`🔓 成功釋放鎖: ${lockKey}`);
            } else {
                console.log(`⚠️  鎖不存在或已過期: ${lockKey}`);
            }

            return result > 0;
        } catch (error) {
            console.error('❌ 釋放鎖失敗:', error);
            throw error;
        }
    }

    /**
     * 檢查鎖是否存在
     * @param {string} key 鎖鍵
     * @returns {Promise<boolean>} 鎖是否存在
     */
    async isLocked(key) {
        try {
            const lockKey = key.startsWith(this.lockPrefix) ? key : this.lockPrefix + key;
            const result = await this.client.exists(lockKey);
            return result === 1;
        } catch (error) {
            console.error('❌ 檢查鎖狀態失敗:', error);
            return false;
        }
    }

    /**
     * 延長鎖的過期時間
     * @param {string} key 鎖鍵
     * @param {number} ttl 新的過期時間（秒）
     * @returns {Promise<boolean>} 是否延長成功
     */
    async extendLock(key, ttl = 120) {
        try {
            const lockKey = key.startsWith(this.lockPrefix) ? key : this.lockPrefix + key;
            const result = await this.client.expire(lockKey, ttl);

            if (result > 0) {
                console.log(`🔄 成功延長鎖: ${lockKey} (新TTL: ${ttl}s)`);
            } else {
                console.log(`⚠️  延長鎖失敗: ${lockKey} (可能已過期)`);
            }

            return result > 0;
        } catch (error) {
            console.error('❌ 延長鎖失敗:', error);
            throw error;
        }
    }

    /**
     * 批量清理過期的鎖
     * @returns {Promise<number>} 清理的鎖數量
     */
    async cleanupExpiredLocks() {
        try {
            const pattern = this.lockPrefix + '*';
            const keys = await this.client.keys(pattern);

            let cleanedCount = 0;
            for (const key of keys) {
                const ttl = await this.client.ttl(key);
                if (ttl === -1) {
                    // 沒有過期時間的鍵，刪除它們
                    await this.client.del(key);
                    cleanedCount++;
                }
            }

            console.log(`🧹 清理了 ${cleanedCount} 個過期鎖`);
            return cleanedCount;
        } catch (error) {
            console.error('❌ 清理過期鎖失敗:', error);
            return 0;
        }
    }

    /**
     * 獲取所有當前鎖的狀態
     * @returns {Promise<Array>} 鎖狀態列表
     */
    async getLockStatus() {
        try {
            const pattern = this.lockPrefix + '*';
            const keys = await this.client.keys(pattern);

            const lockStatus = [];
            for (const key of keys) {
                const ttl = await this.client.ttl(key);
                const value = await this.client.get(key);

                lockStatus.push({
                    key: key.replace(this.lockPrefix, ''),
                    value: value,
                    ttl: ttl,
                    status: ttl > 0 ? 'active' : 'expired'
                });
            }

            return lockStatus;
        } catch (error) {
            console.error('❌ 獲取鎖狀態失敗:', error);
            return [];
        }
    }

    /**
     * 監控鎖的統計信息
     * @returns {Promise<Object>} 統計信息
     */
    async getLockStats() {
        try {
            const lockStatus = await this.getLockStatus();
            
            const stats = {
                totalLocks: lockStatus.length,
                activeLocks: lockStatus.filter(lock => lock.status === 'active').length,
                expiredLocks: lockStatus.filter(lock => lock.status === 'expired').length,
                locksByEpoch: {}
            };

            lockStatus.forEach(lock => {
                const epoch = lock.key;
                if (!stats.locksByEpoch[epoch]) {
                    stats.locksByEpoch[epoch] = {
                        count: 0,
                        status: lock.status
                    };
                }
                stats.locksByEpoch[epoch].count++;
            });

            return stats;
        } catch (error) {
            console.error('❌ 獲取鎖統計信息失敗:', error);
            return {
                totalLocks: 0,
                activeLocks: 0,
                expiredLocks: 0,
                locksByEpoch: {}
            };
        }
    }
}

module.exports = RedisLock;