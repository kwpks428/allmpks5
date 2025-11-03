-- Migration: Drop blocking FKs on claim (and optional bet), keep data flowing
-- Note: Adjust constraint names if your actual FK names differ

BEGIN;

-- 1) Drop FK on hisclaim.epoch (if exists)
ALTER TABLE IF EXISTS hisclaim DROP CONSTRAINT IF EXISTS hisclaim_epoch_fkey;

-- 2) Drop FK on hisclaim.bet_epoch (if exists)
ALTER TABLE IF EXISTS hisclaim DROP CONSTRAINT IF EXISTS hisclaim_bet_epoch_fkey;

-- 3) Optionally drop FK on hisbet.epoch if it blocks inserts
--    If you prefer to keep it, comment this line out.
ALTER TABLE IF EXISTS hisbet DROP CONSTRAINT IF EXISTS hisbet_epoch_fkey;

-- 4) De-dup constraints (idempotent): prefer (tx_hash, log_index, bet_epoch) for claim if single tx can include multiple bet_epoch rows
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relname = 'hisclaim' AND c.conname = 'hisclaim_tx_log_bet_epoch_uniq'
  ) THEN
    BEGIN
      ALTER TABLE hisclaim ADD CONSTRAINT hisclaim_tx_log_bet_epoch_uniq UNIQUE (tx_hash, log_index, bet_epoch);
    EXCEPTION WHEN undefined_column THEN
      -- Fallback: if bet_epoch column not present in the table or not needed, ensure (tx_hash, log_index) uniqueness
      IF NOT EXISTS (
        SELECT 1 FROM pg_constraint c
        JOIN pg_class t ON t.oid = c.conrelid
        WHERE t.relname = 'hisclaim' AND c.conname = 'hisclaim_tx_log_uniq'
      ) THEN
        ALTER TABLE hisclaim ADD CONSTRAINT hisclaim_tx_log_uniq UNIQUE (tx_hash, log_index);
      END IF;
    END;
  END IF;
END $$;

-- 5) hisbet unique constraint
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint c
    JOIN pg_class t ON t.oid = c.conrelid
    WHERE t.relname = 'hisbet' AND c.conname = 'hisbet_tx_log_uniq'
  ) THEN
    ALTER TABLE hisbet ADD CONSTRAINT hisbet_tx_log_uniq UNIQUE (tx_hash, log_index);
  END IF;
END $$;

-- 6) Indexes to support queries (idempotent)
CREATE INDEX IF NOT EXISTS idx_hisclaim_epoch ON hisclaim(epoch);
CREATE INDEX IF NOT EXISTS idx_hisclaim_sender ON hisclaim(sender);
-- bet_epoch is auxiliary, useful for analytics but not the primary axis
CREATE INDEX IF NOT EXISTS idx_hisclaim_bet_epoch ON hisclaim(bet_epoch);

CREATE INDEX IF NOT EXISTS idx_hisbet_epoch ON hisbet(epoch);
CREATE INDEX IF NOT EXISTS idx_hisbet_sender ON hisbet(sender);

COMMIT;
