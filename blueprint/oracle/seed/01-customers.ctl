LOAD DATA
REPLACE
INTO TABLE bronze.customers
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
  customer_id       CHAR(15),
  full_name         CHAR(100),
  nik               CHAR(16),
  phone             CHAR(15),
  email             CHAR(80),
  city              CHAR(30),
  province          CHAR(30),
  registration_date DATE "YYYY-MM-DD",
  kyc_status        CHAR(15),
  risk_score        DECIMAL EXTERNAL,
  segment           CHAR(20)
)