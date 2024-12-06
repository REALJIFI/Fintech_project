

import requests
import time
import logging
import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from log import logging


# Load API key and database URI from .env file
load_dotenv(override=True)


# Get database credentials from environment variables
user = os.getenv("p_user")
password = os.getenv("P_password")
host = os.getenv("P_host")
port = os.getenv("P_port")
dbname = os.getenv("P_database")


API_KEY = os.getenv("API_KEY")  # Replace with your API Key

# Create the PostgreSQL connection string using SQLAlchemy
# DB_URI = os.getenv("DB_URI")  # Replace with your database connection URI
DB_URI = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'


# Constants
BASE_URL = "https://www.alphavantage.co/query"
SYMBOLS = {
    "AAPL": "Apple Inc.",
    "MSFT": "Microsoft Corp.",
    "GOOG": "Alphabet Inc.",
    "AMZN": "Amazon.com Inc.",
    "NFLX": "Netflix Inc."
}
OUTPUT_SIZE = "compact"  # Use "full" if pulling all historical data



# Generate a consistent filename with a timestamp suffix

def generate_output_filename():
    timestamp = pd.Timestamp.now().strftime('%Y%m%d%H%M%S')
    return f"Dataset/raw_data/extracted_data_{timestamp}.csv"




def fetch_stock_data(symbol, api_key):
    """
    Fetch stock data for a given symbol.
    """
    logging.info(f"Fetching data for {symbol}")

    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "apikey": api_key,
        "outputsize": "compact"
    }

    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()

        # Check for API errors
        if "Error Message" in data:
            logging.error(f"API error for {symbol}: {data['Error Message']}")
            return None

        elif "Note" in data:
            logging.warning(f"API limit reached: {data['Note']}")
            time.sleep(60)  # Pause before retrying
            return fetch_stock_data(symbol, api_key)

        return data.get("Time Series (Daily)", {})

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed for {symbol}: {e}")
        return None


def parse_stock_data(symbol, stock_data):
    """
    Parse the JSON data into a structured DataFrame.
    """
    logging.info(f"Parsing data for {symbol}")

    try:
        company_name = SYMBOLS[symbol]
        records = []
        for date, metrics in stock_data.items():
            records.append({
                "Symbol": symbol,
                "CompanyName": company_name,
                "Date": date,
                "OpenPrice": float(metrics.get("1. open", 0)),
                "High": float(metrics.get("2. high", 0)),
                "Low": float(metrics.get("3. low", 0)),
                "ClosePrice": float(metrics.get("4. close", 0)),
                "Volume": int(metrics.get("6. volume", 0))
            })

        df = pd.DataFrame(records)
        logging.info(f"Parsed {len(df)} rows for {symbol}")
        return df

    except Exception as e:
        logging.error(f"Failed to parse data for {symbol}: {e}")
        return pd.DataFrame()


def get_last_processed_date():
    """
    Fetch the most recent DateKey from the database.
    """
    logging.info("Fetching last processed date from the database.")
    engine = create_engine(DB_URI)
    with engine.connect() as conn:
        result = conn.execute(text('SELECT MAX(DateKey) FROM stg.stock_data'))
        last_date = result.scalar()
        if last_date:
            logging.info(f"Last processed date: {last_date}")
        else:
            logging.info("No previously processed date found. Starting fresh.")
    return last_date


def extract_data(api_key):
    """
    Extract stock data for all symbols and save to CSV.
    """
    last_processed_date = get_last_processed_date()

    # Handle first-time extraction explicitly
    if last_processed_date is None:
        logging.info("No last processed date found. Extracting entire dataset.")
        last_processed_date = "2000-01-01"  # Replace with the earliest possible date for your data

    all_data = []

    for symbol in SYMBOLS:
        stock_data = fetch_stock_data(symbol, api_key)
        if stock_data:
            df = parse_stock_data(symbol, stock_data)

            # Filter data for incremental processing
            df["Date"] = pd.to_datetime(df["Date"])
            new_data = df[df["Date"] > pd.to_datetime(last_processed_date)]
            logging.info(f"Identified {len(new_data)} new rows for {symbol}")
            all_data.append(new_data)

        # Respect API rate limits
        time.sleep(12)

    # Combine all data into a single DataFrame
    combined_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

    # Your extraction logic here...
    output_file = f"Dataset/raw_data/extracted_data_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}.csv"

    # Save to CSV
    if not combined_df.empty:
        combined_df.drop_duplicates(subset=["Symbol", "Date"], inplace=True)
        combined_df.to_csv(output_file, index=False)
        logging.info (f"Data successfully extracted and saved to {output_file}")
        
    else:
        logging.warning("No new data extracted. Skipping save step.")

    return output_file  # Return the file path for the next script


if __name__ == "__main__":

    output_file = extract_data(API_KEY)
    print(f"Extracted data saved to: {output_file}")


#if __name__ == "__main__":
 #   extract_data(API_KEY)