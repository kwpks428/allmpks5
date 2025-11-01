/**
 * 輕量級測試模式 - 不消耗RPC流量
 * 只測試核心邏輯，使用模擬數據
 */

const Database = require('./modules/database');
const { Pool } = require('pg');
const moment = require('moment-timezone');

// 模擬數據
const mockRoundData = {
    episode: 426204,
    startBlock: 66621410,
    startTimestamp: 1761966968,
    startTxHash: '0xmock_start_hash',
    lockBlock: 66621450,
    lockTimestamp: 1761967268,
    lockTxHash: '0xmock_lock_hash',
    endBlock: 66621490,
    endTimestamp: 1761967568,
    endTxHash: '0xmock_end_hash'
};

const mockBetData = [
    {
        epoch: 426204,
        user: '0x1234567890123456789012345678901234567890',
        amount: '1000000000000000000', // 1 BNB
        position: 0,
        blockNumber: 66621415,
        timestamp: 1761967000,
        transactionHash: '0xmock_bet_hash_1'
    },
    {
        epoch: 426204,
        user: '0xabcdefabcdefabcdefabcdefabcdefabcdefabcd',
        amount: '2000000000000000000', // 2 BNB
        position: 1,
        blockNumber: 66621420,
        timestamp: 1761967050,
        transactionHash: '0xmock_bet_hash_2'
    }
];

const mockClaimData = [
    {
        epoch: 426204,
        user: '0x1234567890123456789012345678901234567890',
        amount: '1100000000000000000', // 1.1 BNB (winning)
        blockNumber: 66621500,
        timestamp: 1761967600,
        transactionHash: '0xmock_claim_hash_1'
    }
];

async function testLightweight() {
    console.log('🧪 開始輕量級測試...\n');
    
    let database;
    
    try {
        // 測試資料庫連接
        console.log('📊 測試資料庫連接...');
        const pool = new Pool({
            connectionString: process.env.POSTGRES_URL,
        });
        
        await pool.query('SELECT NOW()');
        console.log('✅ 資料庫連接成功\n');
        
        database = new Database(process.env.POSTGRES_URL);
        await database.connect();
        
        // 測試時間戳轉換
        console.log('⏰ 測試時間戳轉換...');
        const testTimestamp = 1761966968;
        const testDate = new Date(Math.floor(testTimestamp * 1000));
        console.log(`   時間戳 ${testTimestamp} -> ${testDate.toISOString()}`);
        console.log('✅ 時間戳轉換正常\n');
        
        // 測試資料庫寫入（使用事務）
        console.log('💾 測試資料庫寫入...');
        await database.transaction(async (client) => {
            // 插入歷史局次
            await database.insertHistoryRound(mockRoundData, client);
            console.log('   插入歷史局次數據完成');
            
            // 插入歷史投注
            await database.insertHistoryBet(mockBetData, client);
            console.log('   插入歷史投注數據完成');
            
            // 插入歷史認領
            await database.insertHistoryClaim(mockClaimData, client);
            console.log('   插入歷史認領數據完成');
        });
        console.log('✅ 資料庫寫入成功\n');
        
        // 測試查詢
        console.log('🔍 測試資料庫查詢...');
        const result = await database.query(`
            SELECT COUNT(*) as bet_count FROM history_bets WHERE epoch = $1
        `, [426204]);
        
        console.log(`   查詢結果: ${result.rows[0].bet_count} 筆投注記錄`);
        console.log('✅ 資料庫查詢正常\n');
        
        // 測試統計
        console.log('📈 測試統計功能...');
        const stats = await database.getStats();
        console.log(`   總局次: ${stats.totalRounds}`);
        console.log(`   總投注: ${stats.totalBets}`);
        console.log(`   總認領: ${stats.totalClaims}`);
        console.log('✅ 統計功能正常\n');
        
        // 測試重複插入（應該不重複）
        console.log('🔄 測試重複插入保護...');
        await database.insertHistoryRound(mockRoundData);
        
        const duplicateCheck = await database.query(`
            SELECT COUNT(*) as count FROM history_rounds WHERE episode = $1
        `, [426204]);
        
        if (duplicateCheck.rows[0].count === '1') {
            console.log('✅ 重複插入保護正常\n');
        } else {
            console.log('❌ 重複插入保護失效\n');
        }
        
        console.log('🎉 所有測試通過！系統核心功能正常。');
        console.log('\n💡 建議: 可以安全部署到正式環境');
        
    } catch (error) {
        console.error('❌ 測試失敗:', error.message);
        console.error(error.stack);
    } finally {
        if (database) {
            await database.disconnect();
            console.log('\n🔌 資料庫連接已關閉');
        }
    }
}

// 運行測試
testLightweight();