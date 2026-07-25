LOAD DATA
REPLACE
INTO TABLE bronze.support_tickets
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
  ticket_id          CHAR(15),
  customer_id        CHAR(15),
  created_at         TIMESTAMP "YYYY-MM-DD HH24:MI:SS",
  resolved_at        TIMESTAMP "YYYY-MM-DD HH24:MI:SS",
  category           CHAR(30),
  priority           CHAR(10),
  satisfaction_score INTEGER EXTERNAL,
  channel            CHAR(15)
)