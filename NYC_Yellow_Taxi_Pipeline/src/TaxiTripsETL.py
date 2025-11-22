import pandas as pd
import os
from src.helpers.Downloader import FileDownloader
from src.constants import PIPELINE_CONFIG
from src.helpers.logger import Logger

logger = Logger(__name__)


class TripsETL:
    def __init__(self) -> None:
        self.config = PIPELINE_CONFIG
        self.downloader = FileDownloader()
    
    def generate_file_names(self) -> list[str]:
        logger.info("Generating file names based on date range")
        start = self.config['data_start']
        end = self.config['data_end']
        start_year, start_month = map(int, start.split('-'))
        end_year, end_month = map(int, end.split('-'))
        file_names = []

        for year in range(start_year, end_year + 1):
            month_start = start_month if year == start_year else 1
            month_end = end_month if year == end_year else 12
            for month in range(month_start, month_end + 1):
                file_name = f'yellow_tripdata_{year:04d}-{month:02d}.parquet'
                file_names.append(file_name)
        return file_names
        
    def download_raw_data(self, file: str) -> None:
        logger.log(f"Downloading raw data file: {file}")
        self.downloader.download_file(
            url=str(self.config['base_url'] + file),
            destination=os.path.join(self.config['raw_data_dir'], file)
        )

    def list_raw_files(self) -> list[str]:
        logger.log("Listing all raw data files")
        raw_data_dir = 'raw_files/'
        return [os.path.join(raw_data_dir, f) for f in os.listdir(raw_data_dir) if f.endswith('.parquet')]
        
    def read_raw_data(self, file_path: str) -> pd.DataFrame:
        logger.log(f"Reading raw data file: {file_path}")
        df = pd.read_parquet(file_path)
        return df
    
    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.log("Processing data: transforming column names")
        df = self.transform_data(df)
        return df

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.log("Transforming column names to standardized format")
        df.rename({
            'VendorID': 'vendor_id',
            'tpep_pickup_datetime': 'pickup_datetime',
            'tpep_dropoff_datetime': 'dropoff_datetime',
            'RatecodeID': 'rate_code_id',
            'PULocationID': 'pickup_location_id',
            'DOLocationID': 'dropoff_location_id',
            'Airport_fee': 'airport_fee'
        },inplace=True, axis=1)
        return df
    
    def validate_data(self, df: pd.DataFrame) -> bool:
        logger.log("Validating data integrity and quality")
        required_columns = [
            'vendor_id', 'pickup_datetime', 'dropoff_datetime',
            'passenger_count', 'trip_distance', 'rate_code_id',
            'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
            'payment_type', 'fare_amount', 'extra', 'mta_tax',
            'tip_amount', 'tolls_amount', 'improvement_surcharge',
            'total_amount', 'congestion_surcharge', 'airport_fee'
        ]
        for col in required_columns:
            if col not in df.columns:
                print(f"Missing required column: {col}")
                return False
        
        assert (df['airport_fee'] >= 0).all(), "airport_fee contains negative values"

        assert df['passenger_count'].between(0, 6).all(), "passenger_count out of range"

        return True
    


    def execute(self) -> None:
        logger.info("Starting ETL process for NYC Yellow Taxi Trips")
        file_names = self.generate_file_names()
        
        for file in file_names:
            if not os.path.exists(os.path.join(self.config['raw_data_dir'], file)):
                self.download_raw_data(file=file)

        files = self.list_raw_files()

        for file in files:

            df = self.read_raw_data(file)
            self.process_data(df)
            self.validate_data(df)
            
    

