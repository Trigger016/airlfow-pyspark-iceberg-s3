
# dev only ?/
from time import time

import os, sys, logging
from argparse import Namespace, ArgumentParser
from pyspark.sql import SparkSession, functions as func
from pyspark.errors import AnalysisException


logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

# remove temp copy of linux runtime env from python
def get_and_purge(key:str) -> str:
    secret = os.getenv(key)
    if not secret:
        logger.error(f'Environment variable not found: {key}')
        raise ValueError('Environment Variable is not found!')

    del os.environ[key]
    return secret

# make ease of argument passing
def arguments() -> Namespace:
    parser = ArgumentParser(description="User Data Processing Job")
    parser.add_argument("--run_date", required=True)
    # parser.add_argument("--first", required=True, default=False)
    return parser.parse_args()

# just in case
def run_first(spark:SparkSession) -> bool:
    try:
        spark.sql(
            """
                CREATE TABLE IF NOT EXISTS silver.account_snapshot (
                    account_id STRING,
                    customer_id STRING,
                    account_type STRING,
                    balance DOUBLE,
                    
                    sum_amount FLOAT,
                    total_transactions INT,
                
                    snapshot_date STRING,
                    last_updated TIMESTAMP
                )
                USING iceberg
                PARTITIONED BY (snapshot_date)
            """
        )
        return True
    except AnalysisException as e:
        logger.error(f'Verify schema and query syntax. Original Exception : {e}')
        return False
    

if __name__ == "__main__":
    try:
        sp = time()

        # configure spark session and read data from oracle db
        args = arguments()
        fetchsize = 50000

        # creds concealment
        jdbc_options = {
            "driver": "oracle.jdbc.OracleDriver",
            "url": f"jdbc:oracle:thin:@{os.getenv('ORACLE_HOST')}:1521/{os.getenv('ORACLE_SERVICE')}",
            "user": get_and_purge('ORACLE_USERNAME'),
            "password": get_and_purge('ORACLE_PASSWORD')
        }
        spark = (
            SparkSession.builder.appName(f"account_snapshot_{args.run_date}")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        if not spark.catalog.tableExists("silver.account_snapshot"):
            logger.warning("Table 'silver.account_snapshot' does not exist.")
            run_first(spark)

        previous_snapshot = (
            spark.read.format("iceberg")
            # .option("split-size", "128MB") # optional
            .load("silver.account_snapshot")
            .where(f"snapshot_date = DATE '{args.run_date}' - INTERVAL '1' DAY")
            .select(
                "account_id",
                "sum_amount",
                "total_transactions"
            )
            # .cache() # unused, will grow as the accounts table grows
        )
        logging.info(f"Previous snapshot loaded.")

        accounts = (
            spark.read.format("jdbc")
            .options(**jdbc_options)
            .option("dbtable", """
                (
                    SELECT
                        account_id,
                        customer_id,
                        account_type,
                        balance
                    FROM ACCOUNTS
                ) account_all
            """)
            .option("fetchsize", fetchsize)
            .load()
        )
        logging.info(f"Accounts table loaded.")

        # filter from the source
        txns = (
            spark.read.format("jdbc")
            .options(**jdbc_options)
            .option("dbtable", f"""
                (
                    SELECT
                        txn_id,
                        account_id,
                        amount
                    FROM TRANSACTIONS
                    WHERE STATUS = 'COMPLETED'
                    AND txn_date >= DATE '{args.run_date}'
                    AND txn_date < DATE '{args.run_date}' + INTERVAL '1' DAY
                ) today_txns
                """)
            .option("fetchsize", fetchsize)
            .load()
        )
        logging.info(f"Transactions table loaded.")

        joined_data = (
            accounts.join(txns, "account_id", "left")
            .groupBy("account_id", "customer_id", "account_type", "balance")
            .agg(
                func.sum("amount").alias("today_sum_amount"),
                func.count("txn_id").alias("today_total_transactions")
            )
        )
        logging.info(f"Data joined and aggregated.")

        today_snapshot = (
            joined_data.join(previous_snapshot, "account_id", "left")
            .withColumn(
                "sum_amount", 
                func.coalesce(func.col("today_sum_amount"), func.lit(0)) + func.coalesce(func.col("sum_amount"), func.lit(0))
            ).withColumn(
                "total_transactions", 
                func.coalesce(func.col("today_total_transactions"), func.lit(0)) + func.coalesce(func.col("total_transactions"), func.lit(0))
            ).withColumn(
                "snapshot_date", 
                func.lit(args.run_date)
            ).withColumn(
                "last_updated", 
                func.current_timestamp()
            ).select(
                "account_id",
                "customer_id",
                "account_type",
                "balance",
                "sum_amount",
                "total_transactions",
                "snapshot_date",
                "last_updated"
            )
        )
        logging.info(f"Today's snapshot prepared.")

        (
            today_snapshot.write
                .format("iceberg") \
                .mode("overwrite") \
                .partitionBy("snapshot_date") \
                .save("silver.account_snapshot")
        )

        logger.info(f"Snapshot job completed in {time() - sp:.2f} seconds.")
    except Exception as e:
        logger.error(f"Job failed with exception: {str(e)}", exc_info=True)
        sys.exit(1)