# main_pipeline.py
from datetime import datetime
from raw_processor import RawProcessor
from transform_processor import TransformProcessor
from custom_stats_processor import CustomStatsProcessor
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
import logging

# Configuration - keep sensitive details out of version control in a real scenario
MYSQL_CONFIG = {
    'host': '  ',
    'database': '  ',
    'user': '  ',
    'password': '  '
}

AWS_CONFIG = {
    'aws_access_key_id': '  ', # Add your AWS Access Key ID
    'aws_secret_access_key': '  ', # Add your AWS Secret Access Key
    'region_name': '  ' # Add your S3 bucket's region
}

BUCKET_NAME = '  ' # Add your S3 bucket name

def run_full_pipeline(**kwargs):
    """Execute the complete IPL Data Pipeline"""
    logging.info(f"\n🏏 IPL Data Pipeline - Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)

    raw_processor_instance = None
    transform_processor_instance = None
    custom_stats_processor_instance = None

    try:
        # Initialize Processors
        logging.info("Initializing processors...")
        raw_processor_instance = RawProcessor(aws_config=AWS_CONFIG, mysql_config=MYSQL_CONFIG, bucket_name=BUCKET_NAME)
        transform_processor_instance = TransformProcessor(mysql_config=MYSQL_CONFIG)
        custom_stats_processor_instance = CustomStatsProcessor(mysql_config=MYSQL_CONFIG)
        logging.info("Processors initialized.")

        # Step 1: Create all tables (RAW, SILVER, GOLD)
        logging.info("\n--- Step 1: Ensuring all table schemas exist ---")
        raw_processor_instance.create_raw_tables()
        transform_processor_instance.create_silver_gold_tables()
        custom_stats_processor_instance.create_custom_gold_tables()
        logging.info("--- Schema creation/verification complete ---")

        # Step 2: Load data from S3 to RAW tables
        logging.info("\n--- Step 2: Loading data from S3 to RAW ---")
        new_matches_loaded_count = raw_processor_instance.load_data_from_s3()
        if new_matches_loaded_count == 0:
            cursor = raw_processor_instance.connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM raw_scorecard")
            raw_data_exists_count = cursor.fetchone()[0]
            cursor.close()
            if raw_data_exists_count == 0:
                logging.info("\n⚠️ No new data loaded from S3 and no existing RAW data found. Pipeline will not proceed further.")
                return
            else:
                logging.info(f"\nℹ️ No new data loaded from S3, but {raw_data_exists_count} existing RAW records found. Proceeding with transformations.")
        logging.info("--- S3 to RAW loading complete ---")

        # Step 3: Transform data from RAW to SILVER
        logging.info("\n--- Step 3: Transforming RAW data to SILVER ---")
        processed_to_silver_count = transform_processor_instance.transform_raw_to_silver()
        if processed_to_silver_count == 0:
            logging.info("\n⚠️ No data was transformed to SILVER layer. GOLD layer transformation will be skipped.")
            cursor = transform_processor_instance.connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM silver_match_summary")
            silver_data_exists_count = cursor.fetchone()[0]
            cursor.close()
            if silver_data_exists_count == 0:
                 logging.info("--- RAW to SILVER transformation complete (no data processed/found) ---")
                 return
            else:
                logging.info(f"ℹ️ No new data processed to SILVER, but {silver_data_exists_count} existing SILVER records found. Proceeding with GOLD.")

        logging.info("--- RAW to SILVER transformation complete ---")
        
        # Step 4: Transform data from SILVER to GOLD
        logging.info("\n--- Step 4: Transforming SILVER data to GOLD ---")
        transform_processor_instance.transform_silver_to_gold()
        logging.info("--- SILVER to GOLD transformation complete ---")

        # Step 5: Calculate and load custom GOLD statistics
        logging.info("\n--- Step 5: Calculating and loading Custom GOLD Statistics ---")
        custom_stats_processor_instance.run_all_custom_stats() # New
        logging.info("--- Custom GOLD Statistics transformation complete ---")

        logging.info("\n🎉 Pipeline execution completed successfully! 🎉")

    except Exception as e:
        logging.error(f"\n❌❌❌ PIPELINE FAILED: {e} ❌❌❌")
        import traceback
        traceback.print_exc()
    finally:
        logging.info("\nClosing database connections...")
        if raw_processor_instance:
            raw_processor_instance.close_connection()
        
        # Check if transform_processor_instance connection is different before closing
        if transform_processor_instance and (not raw_processor_instance or transform_processor_instance.connection != raw_processor_instance.connection):
            transform_processor_instance.close_connection()
        elif transform_processor_instance and raw_processor_instance and transform_processor_instance.connection == raw_processor_instance.connection:
            logging.info("(TransformProcessor) Connection already managed by RawProcessor.")


        # Check if custom_stats_processor_instance connection is different before closing
        if custom_stats_processor_instance:
            is_diff_from_raw = not raw_processor_instance or custom_stats_processor_instance.connection != raw_processor_instance.connection
            is_diff_from_transform = not transform_processor_instance or custom_stats_processor_instance.connection != transform_processor_instance.connection
            
            # Only close if it's a distinct connection object
            if (raw_processor_instance and custom_stats_processor_instance.connection == raw_processor_instance.connection) or \
               (transform_processor_instance and custom_stats_processor_instance.connection == transform_processor_instance.connection):
                logging.info("(CustomStatsProcessor) Connection already managed by another processor.")
            else:
                 custom_stats_processor_instance.close_connection()

        logging.info(f"🏁 Pipeline finished at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    # IMPORTANT: Replace placeholders in AWS_CONFIG before running!
    if AWS_CONFIG['aws_access_key_id'] == 'YOUR_ACCESS_KEY_ID' or \
       AWS_CONFIG['aws_secret_access_key'] == 'YOUR_SECRET_ACCESS_KEY':
        logging.info("🚨 CRITICAL ERROR: AWS credentials are placeholders in main_pipeline.py.")
        logging.info("🚨 Please replace 'YOUR_ACCESS_KEY_ID' and 'YOUR_SECRET_ACCESS_KEY' before running.")
    else:
        run_full_pipeline()
