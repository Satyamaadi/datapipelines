from src.TaxiTripsETL import TripsETL

if __name__ == "__main__":
    etl = TripsETL()
    etl.execute()