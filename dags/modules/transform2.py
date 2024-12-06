


import pandas as pd
import uuid
import logging
import os
import glob
from log import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load API key and database URI from .env file
load_dotenv(override=True)

# Get database credentials from environment variables
user = os.getenv("P_user")
password = os.getenv("P_password")
host = os.getenv("P_host")
port = os.getenv("P_port")
dbname = os.getenv("P_database")

DB_URI = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'

# Company-to-ID mapping
COMPANY_ID_MAP = {
    "Apple Inc.": 1,
    "Microsoft Corp.": 2,
    "Alphabet Inc.": 3,
    "Amazon.com Inc.": 4,
    "Netflix Inc.": 5
}

def generate_record_id():
    """Generate a unique UUID for each record."""
    return str(uuid.uuid4())

def map_company_id(company_name):
    """Map company names to their corresponding IDs."""
    return COMPANY_ID_MAP.get(company_name, None)


def get_last_processed_date():

    """Fetch the most recent DateKey from the database."""
    logging.info("Fetching last processed date from the database.")
    try:
        engine = create_engine(DB_URI)
        with engine.connect() as conn:
            result = conn.execute(text('SELECT MAX(DateKey) FROM stg.stock_data'))
            last_date = result.scalar()
            if last_date:
                logging.info(f"Last processed date: {last_date}")
            else:
                logging.info("No previously processed date found. Starting fresh.")
        return last_date
    except Exception as e:
        logging.error(f"Error fetching last processed date: {e}")
        raise

def get_latest_file(directory, prefix):

    """Find the latest file in the directory with the given prefix."""
    files = glob.glob(f"{directory}/{prefix}*.csv")
    if not files:
        logging.error(f"No files found with prefix {prefix} in directory {directory}")
        raise FileNotFoundError(f"No files found with prefix {prefix} in directory {directory}")
    latest_file = max(files, key=os.path.getctime)
    logging.info(f"Latest file found: {latest_file}")
    return latest_file


def transform_data(input_file, output_file, last_processed_date=None):

    """
    Transform the extracted data to align with the staging schema.

    """
    logging.info("Starting transformation process")
    try:
        df = pd.read_csv(input_file)
        logging.info(f"Loaded {len(df)} rows from {input_file}")

        # Filter data based on last processed date
        if last_processed_date:
            df['Date'] = pd.to_datetime(df['Date'])
            initial_count = len(df)
            df = df[df['Date'] > pd.to_datetime(last_processed_date)]
            logging.info(f"Filtered {initial_count - len(df)} rows processed before {last_processed_date}")

        else:
            logging.info("Processing entire dataset as no last processed date was provided.")


        # Fill missing values
        df.fillna({
            'OpenPrice': 0.0,
            'High': 0.0,
            'Low': 0.0,
            'ClosePrice': 0.0,
            'Volume': 0
        }, inplace=True)


        # Generate UUIDs and map Company IDs
        df['RecordID'] = [generate_record_id() for _ in range(len(df))]
        df['CompanyID'] = df['CompanyName'].apply(map_company_id)
        df['DateID'] = pd.to_datetime(df['Date']).dt.strftime('%Y%m%d').astype(int)


        # Ensure proper data types
        df = df.astype({
            'OpenPrice': 'float',
            'High': 'float',
            'Low': 'float',
            'ClosePrice': 'float',
            'Volume': 'int',
            'Symbol': 'string',
            'CompanyName': 'string'
        })


        # Remove duplicate rows
        before_dedup = len(df)
        df.drop_duplicates(subset=['Symbol', 'Date'], inplace=True)
        duplicates_removed = before_dedup - len(df)
        logging.info(f"Removed {duplicates_removed} duplicate rows")


        # Calculate Daily Return

        logging.info("Calculating DailyReturn")

        df['PreviousClosePrice'] = df.groupby('Symbol')['ClosePrice'].shift(1)
        df['DailyReturn'] = (df['ClosePrice'] - df['PreviousClosePrice']) / df['PreviousClosePrice']
        df['DailyReturn'].fillna(0, inplace=True)  # Handle NaN values for the first day or missing data


        # Reorganize DataFrame
        transformed_df = df[[ 
            'RecordID', 'CompanyID', 'Symbol', 'CompanyName', 'DateID',
            'Date', 'OpenPrice', 'High', 'Low', 'ClosePrice', 'Volume', 'DailyReturn'
        ]]

        logging.info(f"Transformed data contains {len(transformed_df)} rows")
        transformed_df.to_csv(output_file, index=False)
        logging.info(f"Transformation complete. Saved to {output_file}")

    except Exception as e:
        logging.error(f"Error during transformation: {e}")
        raise


if __name__ == "__main__":
    
    raw_data_dir = "Dataset/raw_data"
    cleaned_data_dir = "Dataset/cleaned_data"
    prefix = "extracted_data_"

    # Get the latest extracted file
    input_file = get_latest_file(raw_data_dir, prefix)

    # Define output file with timestamp
    timestamp = pd.Timestamp.now().strftime('%Y%m%d%H%M%S')
    output_file = f"{cleaned_data_dir}/transformed_data_{timestamp}.csv"

    # Ensure output directory exists
    os.makedirs(cleaned_data_dir, exist_ok=True)

    # Get last processed date
    last_processed_date = get_last_processed_date()

    # Perform transformation
    transform_data(input_file, output_file, last_processed_date)
