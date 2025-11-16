import pandas as pd
import os


class TripsETL:
    def __init__(self) -> None:
        pass

    def list_raw_files(self) -> list[str]:
        raw_data_dir = 'raw_files/'
        return [os.path.join(raw_data_dir, f) for f in os.listdir(raw_data_dir) if f.endswith('.parquet')]
        
    def read_raw_data(self, file_path: str) -> pd.DataFrame:
        df = pd.read_parquet(file_path)
        return df
    
    def process_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.transform_data(df)
        return df

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
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
        
        assert df['airport_fee'] >= 0, "airport_fee contains negative values"

        assert df['passenger_count'].between(0, 6).all(), "passenger_count out of range"

        return True
    


    def execute(self) -> None:
        files = self.list_raw_files()
        for file in files:
            df = self.read_raw_data(file)
            self.process_data(df)
            self.validate_data(df)
            
    

