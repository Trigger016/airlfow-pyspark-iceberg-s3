LOAD DATA
REPLACE
INTO TABLE bronze.transactions
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
  txn_id            CHAR(25),
  account_id        CHAR(20),
  txn_date          TIMESTAMP "YYYY-MM-DD HH24:MI:SS",
  txn_type          CHAR(20),
  amount            DECIMAL EXTERNAL,
  merchant_category CHAR(30),
  channel           CHAR(20),
  reference_id      CHAR(30),
  status            CHAR(15)
)