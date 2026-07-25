LOAD DATA
REPLACE
INTO TABLE bronze.app_events
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
  event_id          CHAR(30),
  customer_id       CHAR(15),
  event_timestamp   TIMESTAMP "YYYY-MM-DD HH24:MI:SS",
  event_type        CHAR(30),
  screen_name       CHAR(50),
  session_id        CHAR(30),
  device_type       CHAR(15),
  app_version       CHAR(10)
)