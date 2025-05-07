# main_pipeline.py
from datetime import datetime
from raw_processor import RawProcessor
from transform_processor import TransformProcessor

class IPLPipeline:
    def run_pipeline(self):
        print(f"\n🏏 IPL Pipeline - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*50)
        
        try:
            # Load RAW data
            raw_processor = RawProcessor()
            loaded = raw_processor.load_data_from_s3()
            
            if loaded == 0:
                print("\n⚠️ No new data - exiting")
                return
            
            # Process transformations
            transformer = TransformProcessor()
            transformer.transform_raw_to_silver()
            transformer.transform_silver_to_gold()
            
            print("\n🎯 Pipeline completed successfully!")
            
        except Exception as e:
            print(f"\n❌ Pipeline failed: {e}")
        finally:
            if hasattr(raw_processor, 'connection'):
                raw_processor.connection.close()
            print("MySQL connections closed")

if __name__ == "__main__":
    IPLPipeline().run_pipeline()