LOAD DATA
REPLACE
INTO TABLE bronze.credit_scores
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
  score_id               CHAR(20),
  customer_id            CHAR(15),
  score_date             DATE "YYYY-MM-DD",
  model_version          CHAR(10),
  credit_score           INTEGER EXTERNAL,
  probability_of_default DECIMAL EXTERNAL,
  features_used          CHAR(4000)
)