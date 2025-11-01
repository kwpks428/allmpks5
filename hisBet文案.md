# 🥞 PancakeSwap BNB/USD 預測遊戲歷史數據抓取系統設計文件 (最終版)

> **版本**：v2.2 (含錯誤日誌)
> **核心技術**：Node.js + ethers.js + Redis
> **目標**：雙軌數據抓取、併發控制、事務性寫入、數據完整性驗證
> **規範**：所有命名強制駝峰式 (camelCase)

---

## 1. 🔗 基礎資訊

| 項目 | 值 |
| :--- | :--- |
| **合約地址** | `0x18B2A687610328590Bc8F2e5fEdDe3b582A49cdA` |
| **ABI 路徑** | `./abi.json` |
| **BSC RPC Endpoint** | `https://lb.drpc.live/bsc/Ahc3I-33qkfGuwXSahR3XfMHZiZ_sBMR8Lq_QrxF2MGT` |
| **RPC 支援** | 單次請求最多 100,000 個區塊 |
| **統一時區** | 台北時間 (UTC+8) |
| **時間格式** | `YYYY-MM-DD HH:mm:ss` (無毫秒、無時區標示) |

---

## 2. 🧩 系統架構：主線、支線與併發控制

### ✅ 2.1 主線 (Main Thread)
* **觸發**：程式啟動時立即執行。
* **任務**：取得當前最新局次 `currentEpoch`。從 `currentEpoch - 2` 開始**持續向歷史回溯** (`n-2`, `n-3`, `n-4`, ...)。
* **執行邏輯**：對於**每一局** (`n-2`, `n-3`...)，均執行以下檢查：
    1.  **檢查 `finEpoch` 表**：若該局 `epoch` 已存在，則**跳過此局，繼續處理下一局** (`epoch - 1`)。
    2.  **檢查 `Redis` 鎖**：若該局 `epoch` 已被鎖定（詳見 2.5 節），則**跳過此局，繼續處理下一局** (`epoch - 1`)。
    3.  **執行抓取**：若 1 和 2 均通過，則開始執行抓取、驗證與寫入流程。
* **重啟**：每 **30 分鐘進行一次優雅重啟**。

### ✅ 2.2 支線 (Secondary Thread)
* **觸發**：程式啟動後 **5 分鐘首次觸發**，之後**每 5 分鐘執行一次**。
* **任務**：僅負責 `currentEpoch - 2`, `currentEpoch - 3`, `currentEpoch - 4` 這三局。
* **執行邏輯**：對於這三局中的**每一局**，均執行以下檢查：
    1.  **檢查 `finEpoch` 表**：若該局 `epoch` 已存在，則**跳過此局**。
    2.  **檢查 `Redis` 鎖**：若該局 `epoch` 已被鎖定，則**跳過此局**。
    3.  **執行抓取**：若 1 和 2 均通過，則執行抓取、驗證與寫入流程。

### ✅ 2.5 ⚡ 併發控制 (Redis 鎖)

為防止主線與支線在同一時刻抓取同一局 `epoch`（Race Condition），引入 Redis 鎖機制。

* **鎖 Key 格式**：`lock:pancake:epoch:{epoch}` (例如: `lock:pancake:epoch:483750`)
* **鎖 TTL (過期時間)**：`120` 秒 (2 分鐘)。
    * *（備註：此 TTL 應略大於抓取+驗證+寫入單局所需的平均時間，以防腳本意外崩潰導致死鎖）*

#### 執行流程：
1.  **檢查 `finEpoch`**：(如 2.1 / 2.2 所述)。若存在，跳過。
2.  **嘗試獲取鎖**：(若 `finEpoch` 不存在)
    * 執行 Redis `SET` 命令並帶上 `NX` (Not Exists) 和 `EX` (Expire) 參數。
    * 例如：`SET lock:pancake:epoch:483750 processing NX EX 120`
3.  **判斷鎖狀態**：
    * **獲取成功** (`SET` 返回 `OK` 或 `1`)：表示此線程已取得處理權。立即開始執行[數據抓取](#3-數據抓取策略二分搜尋--區塊範圍定位)。
    * **獲取失敗** (`SET` 返回 `nil` 或 `0`)：表示該 `epoch` 已被另一線程鎖定並正在處理中。**立即跳過此局**。
4.  **釋放鎖**：
    * 在事務**成功提交**後，**或**在流程中**發生任何錯誤**導致中斷時，**都必須**在 `finally` 區塊中執行 `DEL` 來釋放鎖。

---

## 3. 🔍 數據抓取策略：二分搜尋 + 區塊範圍定位

對每一目標局次 `epoch`：

1.  取得該局的 `startTime` 與下一局的 `startTime`，作為時間查詢區間 `[start, nextStart)`。
2.  使用 **二分搜尋法** 找出對應的起始與結束區塊號。
3.  利用 RPC **單次最大 10 萬區塊批量查詢能力**，一次性獲取此區間內所有相關事件日誌（`LockEvent`, `CloseRoundEvent`, `BetEvent` (BetBull/BetBear), `ClaimEvent`）。
4.  因只抓 `currentEpoch - 2` 以後的數據，所有回合均已結算，無未完成狀態風險。

---

## 4. 🗂️ 資料結構定義 (強制 camelCase)

> **規範**：所有表與欄位均使用 `camelCase`。禁止添加 `id`, `hash`, `createdAt`, `updatedAt` 等任何未提及的欄位。

### 4.1 `round` 表
| 欄位名稱 | 類型 | 說明 |
| :--- | :--- | :--- |
| epoch | `BIGINT` | 局次編號 (Primary Key) |
| startTime | `DATETIME` | 開始時間 (台北時區) |
| lockTime | `DATETIME` | 鎖倉時間 (台北時區) |
| closeTime | `DATETIME` | 結束時間 (台北時區) |
| lockPrice | `DECIMAL(18,8)` | 鎖倉價格 (USD) |
| closePrice | `DECIMAL(18,8)` | 收盤價格 (USD) |
| result | `ENUM('up','down')` | 結果 |
| totalAmount | `DECIMAL(18,8)` | 總下注金額 (BNB) |
| upAmount | `DECIMAL(18,8)` | 看漲方下注總額 |
| downAmount | `DECIMAL(18,8)` | 看跌方下注總額 |
| upOdds | `DECIMAL(18,4)` | 看漲賠率 |
| downOdds | `DECIMAL(18,4)` | 看跌賠率 |

> **賠率計算公式**：
> ```
> poolAfterFee = totalAmount * 0.97
> upOdds = poolAfterFee / upAmount
> downOdds = poolAfterFee / downAmount
> ```

### 4.2 `hisBet` 表
| 欄位名稱 | 類型 | 說明 |
| :--- | :--- | :--- |
| epoch | `BIGINT` | 對應局次 |
| betTime | `DATETIME` | 下注時間 (台北時區) |
| walletAddress | `VARCHAR(42)` | 錢包地址 (全小寫) |
| betDirection | `ENUM('up','down')` | 下注方向 |
| betAmount | `DECIMAL(18,8)` | 下注金額 (BNB) |
| result | `ENUM('win','loss')` | 結果 |
| blockNumber | `BIGINT` | 發生區塊號 |

### 4.3 `claim` 表
| 欄位名稱 | 類型 | 說明 |
| :--- | :--- | :--- |
| epoch | `BIGINT` | 觸發提領動作的當前局次 |
| walletAddress | `VARCHAR(42)` | 提領者的錢包地址 |
| betEpoch | `BIGINT` | 實際要領取獎金的歷史局次 |
| claimAmount | `DECIMAL(18,8)` | 領取金額 (BNB) |
> **唯一索引**：`UNIQUE(epoch, walletAddress, betEpoch)`

### 4.4 `multiClaim` 表 (巨鯨行為偵測)
| 欄位名稱 | 類型 | 說明 |
| :--- | :--- | :--- |
| epoch | `BIGINT` | 觸發提領的局次 |
| walletAddress | `VARCHAR(42)` | 錢包地址 |
| claimCount | `INT` | 本次共領取多少局的獎金 |
| totalAmount | `DECIMAL(18,8)` | 總領取金額 (BNB) |

### 4.5 `realBet` 表 (臨時/即時數據表)
| 欄位名稱 | 類型 | 說明 |
| :--- | :--- | :--- |
| epoch | `BIGINT` | 對應局次 |
| betTime | `DATETIME` | 下注時間 (台北時區) |
| walletAddress | `VARCHAR(42)` | 錢包地址 (全小寫) |
| betDirection | `ENUM('up','down')` | 下注方向 |
| betAmount | `DECIMAL(18,8)` | 下注金額 (BNB) |
| blockNumber | `BIGINT` | 發生區塊號 |
> **用途**：由其他即時系統寫入。本歷史抓取系統的職責是在事務中**清除**此表中對應 `epoch` 的數據。

### 4.6 `finEpoch` 表 (完成標記)
| 欄位名稱 | 類型 | 說明 |
| :--- | :--- | :--- |
| epoch | `BIGINT` | 已完整驗證並成功寫入的局次 (Primary Key) |
> **用途**：主線與支線啟動時查詢此表，跳過已完成的局次。

### 4.7 `errEpoch` 表 (錯誤日誌表)
| 欄位名稱 | 類型 | 說明 |
| :--- | :--- | :--- |
| epoch | `BIGINT` | 抓取失敗的局次 (Primary Key) |
| errorTime | `DATETIME` | 錯誤發生時間 (台北時區) |
| errorMessage | `TEXT` | 錯誤訊息（例如：驗證失敗、RPC 錯誤、DB 錯誤）|
> **用途**：在 `catch` 區塊中捕獲到任何處理錯誤時，將該 `epoch` 及錯誤訊息寫入此表。
> **注意**：寫入此表應**獨立於主事務**，即使主事務回滾，錯誤日誌也應成功寫入。

---

## 5. ✅ 數據完整性驗證流程 (事務前執行)

在準備寫入資料庫前，必須對該 `epoch` 的數據完成以下驗證：

1.  **`round` 表完整性**
    * 所有欄位非空 (Not Null)。
    * `upAmount > 0` 且 `downAmount > 0`。
    * `Math.abs(totalAmount - (upAmount + downAmount)) <= 0.0001`。
    * 賠率計算正確。

2.  **`hisBet` 總額核對**
    * 計算該 `epoch` 所有 `hisBet.betAmount` 總和。
    * 核對是否接近 `round.totalAmount` (**誤差 ≤ 0.001 BNB**)。

3.  **`claim` 表完整性驗證**
    * **驗證規則**：根據業務確認，每一局已結算的 `epoch` 都**必定有領獎資料**。
    * **錯誤處理**：必須驗證抓取到的 `claim` 資料**不得為空** (`claimData.length > 0`)。如果為 0 筆，代表抓取邏輯有誤或 RPC 數據不全，應**立即中斷**此局處理、記錄嚴重錯誤，並**釋放 Redis 鎖**（以便重試）。

4.  **`multiClaim` 資料生成**
    * 根據 `claim` 表聚合每位用戶在單一 `epoch` 的提領筆數與總額。
    * **觸發條件** (任一成立即記錄)：
        * 同一錢包在同一 `epoch` 提領局數 `claimCount >= 5`
        * 或總提領金額 `totalAmount >= 1 BNB`

---

## 6. 🛑 事務寫入與覆蓋機制

數據抓取與驗證完成後（前提是已通過 `finEpoch` 檢查與 `Redis` 鎖），嚴格執行以下事務性寫入流程：

1.  **準備事務**：啟動資料庫事務 (Transaction)。
2.  **清理臨時數據 (僅 realBet)**：在事務內，**首先且僅刪除 `realBet` 表中**所有 `epoch` 欄位等於當前目標局次的臨時數據。
3.  **批量寫入**：在事務內，將驗證通過的 `roundData`, `hisBetData`, `claimData`, `multiClaimData` (若有) 批量插入對應表格。
    * *（註：此處依賴 `finEpoch` 檢查來保證 `round` 等表的主鍵不重複。若因意外導致主鍵衝突，事務將自動失敗並回滾。）*
4.  **標記完成 (僅 finEpoch)**：在事務內，**僅將**該 `epoch` 寫入 `finEpoch` 表，標記本歷史抓取系統已完成此局。
5.  **提交事務**：提交 (Commit) 事務。若中途任一步驟失敗（包括主鍵衝突），則**自動回滾 (Rollback)**，確保數據一致性。

---

## 7. 💻 事務程式碼範例 (示意)

```javascript
// (假設已成功獲取 Redis 鎖)
try {
  // ... (執行 3. 抓取 和 5. 驗證) ...
  // (假設 5. 驗證通過)

  // 驗證通過，開始事務
  await db.transaction(async (trx) => {
    
    // 2. 清理臨時數據 (僅 realBet)
    await trx('realBet').where({ epoch }).del();

    // 3. 批量寫入新資料
    await trx.insert(roundData).into('round');
    await trx.insert(hisBetData).into('hisBet');
    await trx.insert(claimData).into('claim'); // 已通過驗證，不為空
    
    if (multiClaimData.length > 0) {
      await trx.insert(multiClaimData).into('multiClaim');
    }

    // 4. 標記完成 (僅寫入 finEpoch)
    await trx.insert({ epoch }).into('finEpoch');
  });

} catch (error) {
  // 記錄錯誤 (例如驗證失敗、主鍵衝突等)
  console.error(`處理 Epoch ${epoch} 失敗:`, error);
  
  // (新增) 寫入錯誤日誌
  try {
    await db('errEpoch').insert({
      epoch: epoch,
      errorTime: new Date(), // 確保轉換為台北時區 YYYY-MM-DD HH:mm:ss
      errorMessage: error.message || error.toString()
    }).onConflict('epoch').merge(); // 如果已存在則更新錯誤訊息
  } catch (logError) {
    console.error(`寫入 errEpoch 失敗:`, logError);
  }
  
  // 事務已自動回滾 (ROLLBACK)

} finally {
  // 釋放鎖 (無論成功或失敗)
  await redis.del(`lock:pancake:epoch:${epoch}`);
}

## 8. 📈 總結流程 (主線與支線共用)

1. 決定目標 epoch (主線 n-2... / 支線 n-2, n-3, n-4)
2. 查詢 finEpoch → 若 epoch 存在 → 跳過 (處理下一局)
3. (若不存在) 嘗試獲取 Redis 鎖 (SET NX EX)
4.   → 獲取失敗 (鎖已存在) → 跳過 (處理下一局)
5. (若獲取成功) --> 進入 try...finally 區塊
6.   try {
7.     a. 定位區塊範圍 (二分法)
8.     b. 批量抓取並解析事件 (round, hisBet, claim)
9.     c. 執行數據完整性驗證 (round, hisBet 總額, claim 不為空)
10.    d. 產生 multiClaim 資料 (如有)
11.    e. (若驗證通過) 事務開始 (BEGIN TRANSACTION)
12.    f.   清理即時數據 (DELETE from realBet WHERE epoch = ?)
13.    g.   寫入歷史數據 (INSERT into round, hisBet, claim, multiClaim)
14.    h.   寫入完成標記 (INSERT into finEpoch)
15.    i. 提交事務 (COMMIT)
16.  } catch (error) {
17.    // 記錄錯誤
18.    // (新增) 寫入錯誤日誌 (INSERT into errEpoch ...)
19.    // 事務已自動回滾 (ROLLBACK)
20.  } finally {
21.    // 無論成功或失敗，都必須釋放鎖
22.    釋放 Redis 鎖 (DEL lock:pancake:epoch:{epoch})
23.  }
24. 處理下一局