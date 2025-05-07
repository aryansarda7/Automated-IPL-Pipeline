# raw_processor.py
import json
import boto3
import mysql.connector
from mysql.connector import Error
from config import MYSQL_CONFIG, AWS_CONFIG, BUCKET_NAME

class RawProcessor:
    def __init__(self):
        self.s3 = boto3.client('s3', **AWS_CONFIG)
        self.connection = None
        self._create_db_connection()

    def _create_db_connection(self):
        """Create MySQL database connection"""
        try:
            self.connection = mysql.connector.connect(**MYSQL_CONFIG)
            print("✅ RAW: Connected to MySQL")
        except Error as e:
            print(f"❌ RAW: Connection error: {e}")
            raise

    def _execute_sql(self, query, params=None):
        """Execute SQL query for RAW operations"""
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            self.connection.commit()
        except Error as e:
            print(f"❌ RAW: SQL Error: {e}")
            self.connection.rollback()
            raise
        finally:
            cursor.close()

    def load_data_from_s3(self):
        """Load data from S3 to RAW tables"""
        try:
            print("\n🔍 RAW: Listing match folders...")
            response = self.s3.list_objects_v2(Bucket=BUCKET_NAME, Delimiter='/')
            
            if 'CommonPrefixes' not in response:
                print("⚠️ RAW: No match folders found!")
                return 0
                
            folders = [prefix['Prefix'] for prefix in response['CommonPrefixes']]
            print(f"📂 RAW: Found {len(folders)} folders")
            
            total_loaded = 0
            for folder in folders:
                match_id = folder.rstrip('/')
                if self._check_existing(match_id):
                    continue
                
                # Load scorecard
                scard_key = f"{folder}{match_id}_scard.json"
                scard_data = self._load_s3_json(scard_key)
                self._insert_raw_data('raw_scorecard', match_id, scard_key, scard_data)
                
                # Try commentary
                try:
                    comm_key = f"{folder}{match_id}_comm.json"
                    comm_data = self._load_s3_json(comm_key)
                    self._insert_raw_data('raw_commentary', match_id, comm_key, comm_data)
                    print(f"✅ RAW: Loaded with commentary: {match_id}")
                except:
                    print(f"✅ RAW: Loaded without commentary: {match_id}")
                
                total_loaded += 1
            
            print(f"\n📊 RAW: Loaded {total_loaded} new matches")
            return total_loaded
            
        except Exception as e:
            print(f"❌ RAW: S3 loading error: {e}")
            raise

    def _check_existing(self, match_id):
        """Check if match already exists"""
        cursor = self.connection.cursor()
        cursor.execute("SELECT 1 FROM raw_scorecard WHERE match_id = %s", (match_id,))
        exists = cursor.fetchone()
        cursor.close()
        if exists:
            print(f"⏩ RAW: Skipping existing {match_id}")
            return True
        return False

    def _load_s3_json(self, key):
        """Load JSON data from S3"""
        obj = self.s3.get_object(Bucket=BUCKET_NAME, Key=key)
        return json.loads(obj['Body'].read().decode('utf-8'))

    def _insert_raw_data(self, table, match_id, file_name, data):
        """Insert data into RAW table"""
        self._execute_sql(
            f"INSERT INTO {table} (match_id, file_name, json_data) VALUES (%s, %s, %s)",
            (match_id, file_name, json.dumps(data))
        )