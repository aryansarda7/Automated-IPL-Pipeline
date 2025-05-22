# raw_processor.py
import os
import json
import boto3
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin

bucket_name = '  '

class RawProcessor(LoggingMixin):
    def __init__(self, aws_config=None, mysql_config=None, bucket_name=None):
        # Default configs (can be overridden)
        self.aws_config = aws_config or {
            'aws_access_key_id': '  ',
            'aws_secret_access_key': '  ',
            'region_name': '  '
        }
        self.mysql_config = mysql_config or {
            'host': '  ',
            'database': '  ',
            'user': '  ',
            'password': '  '
        }
        self.bucket_name = bucket_name
        self.s3 = None
        self.connection = None
        self._initialize_clients()

    def _initialize_clients(self):
        try:
            self.s3 = boto3.client('s3', **self.aws_config)
            self._create_db_connection()
        except Exception as e:
            self.log.error(f"Initialization error in RawProcessor: {e}")
            raise AirflowException(f"RawProcessor initialization failed: {e}")

    def _create_db_connection(self):
        """Create and return MySQL database connection"""
        try:
            if self.connection is None or not self.connection.is_connected():
                self.connection = mysql.connector.connect(**self.mysql_config)
                self.log.info("Successfully connected to MySQL database")
            return self.connection
        except Error as e:
            self.log.error(f"Error connecting to MySQL: {e}")
            raise AirflowException(f"MySQL connection failed: {e}")

    def _execute_sql(self, query, params=None, multi=False):
        """Execute SQL query with error handling"""
        if not self.connection or not self.connection.is_connected():
            self._create_db_connection()
        
        cursor = self.connection.cursor()
        try:
            if multi:
                for result in cursor.execute(query, params, multi=True):
                    if result.with_rows:
                        self.log.info(f"Executed: {result.statement}")
            else:
                cursor.execute(query, params)
            self.connection.commit()
        except Error as e:
            self.log.error(f"SQL Error: {e}\nQuery: {query}\nParams: {params}")
            self.connection.rollback()
            raise AirflowException(f"SQL execution failed: {e}")
        finally:
            cursor.close()

    def create_raw_tables(self):
        """Create RAW layer tables"""
        try:
            self._execute_sql("""
                CREATE TABLE IF NOT EXISTS raw_commentary (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    match_id VARCHAR(100),
                    file_name VARCHAR(255),
                    json_data JSON,
                    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_match_comm (match_id)
                )
            """)

            self._execute_sql("""
                CREATE TABLE IF NOT EXISTS raw_scorecard (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    match_id VARCHAR(100),
                    file_name VARCHAR(255),
                    json_data JSON,
                    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_match_scard (match_id)
                )
            """)
            self.log.info("RAW tables created/verified successfully")
            return True
        except Error as e:
            self.log.error(f"Error creating RAW tables: {e}")
            raise AirflowException(f"Table creation failed: {e}")

    def load_data_from_s3(self):
        """Load data from S3 with detailed progress tracking"""
        try:
            self.log.info("Listing match folders from S3...")
            response = self.s3.list_objects_v2(Bucket=self.bucket_name, Delimiter='/')
            
            if 'CommonPrefixes' not in response:
                self.log.warning("No match folders found in S3 bucket!")
                return 0
                
            folders = [prefix['Prefix'] for prefix in response.get('CommonPrefixes', [])]
            self.log.info(f"Found {len(folders)} match folders in S3 bucket")
            
            total_loaded = 0
            skipped_existing = 0
            
            for folder in folders:
                match_id_from_folder = folder.rstrip('/')
                
                try:
                    # Check if match already exists
                    cursor = self.connection.cursor()
                    cursor.execute("SELECT 1 FROM raw_scorecard WHERE match_id = %s", (match_id_from_folder,))
                    if cursor.fetchone():
                        self.log.info(f"Skipping already loaded match: {match_id_from_folder}")
                        skipped_existing += 1
                        cursor.close()
                        continue
                    cursor.close()
                    
                    # Load scorecard data
                    scard_key = f"{folder}{match_id_from_folder}_scard.json"
                    obj = self.s3.get_object(Bucket=self.bucket_name, Key=scard_key)
                    scard_data = json.loads(obj['Body'].read().decode('utf-8'))
                    
                    self._execute_sql(
                        "INSERT INTO raw_scorecard (match_id, file_name, json_data) VALUES (%s, %s, %s)",
                        (match_id_from_folder, scard_key, json.dumps(scard_data))
                    )
                    
                    # Try to load commentary if exists
                    try:
                        comm_key = f"{folder}{match_id_from_folder}_comm.json"
                        obj = self.s3.get_object(Bucket=self.bucket_name, Key=comm_key)
                        comm_data = json.loads(obj['Body'].read().decode('utf-8'))
                        
                        self._execute_sql(
                            "INSERT INTO raw_commentary (match_id, file_name, json_data) VALUES (%s, %s, %s)",
                            (match_id_from_folder, comm_key, json.dumps(comm_data))
                        )
                        self.log.info(f"Loaded match with commentary: {match_id_from_folder}")
                    except Exception as e:
                        if hasattr(e, 'response') and e.response.get('Error', {}).get('Code') == 'NoSuchKey':
                             self.log.info(f"Loaded match (no commentary file found): {match_id_from_folder}")
                        else:
                            self.log.warning(f"Warning loading commentary for {match_id_from_folder} (scorecard loaded): {str(e)}")
                    
                    total_loaded += 1
                    
                except Exception as e:
                    self.log.error(f"Failed to process {match_id_from_folder}: {str(e)}")
                    continue
            
            self.log.info(f"Loaded {total_loaded} new matches, skipped {skipped_existing} existing matches")
            return total_loaded
            
        except Exception as e:
            self.log.error(f"S3 loading error: {e}")
            raise AirflowException(f"S3 loading failed: {e}")

    def close_connection(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.log.info("MySQL connection closed")
