LOAD DATA
REPLACE
INTO TABLE bronze.accounts
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
  account_id    CHAR(20),
  customer_id   CHAR(15),
  account_type  CHAR(20),
  product_name  CHAR(50),
  opened_date   DATE "YYYY-MM-DD",
  status        CHAR(15),
  balance       DECIMAL EXTERNAL,
  credit_limit  DECIMAL EXTERNAL,
  interest_rate DECIMAL EXTERNAL
)