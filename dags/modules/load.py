

import os
import glob
import pandas as pd
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError
from datetime import datetime
from log import logging


def create_dimension_and_fact_tables(engine):
    """
    Creates the dimension and fact tables if they do not exist.
    """
    try:
        with engine.begin() as connection:
            logging.info("Ensuring schema EDW exists.")
            connection.execute(text('CREATE SCHEMA IF NOT EXISTS "EDW";'))
            connection.execute(text('CREATE SCHEMA IF NOT EXISTS "STG";'))

            # Create Dimension Tables
            logging.info("Creating dim_company table.")
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS "EDW".dim_company (
                    CompanyID SERIAL PRIMARY KEY,
                    Symbol VARCHAR(10) UNIQUE NOT NULL,
                    CompanyName VARCHAR(255) NOT NULL
                );
            """))

            logging.info("Creating dim_date table.")
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS "EDW".dim_date (
                    DateID DATE PRIMARY KEY,
                    Year INT NOT NULL,
                    Month INT NOT NULL,
                    Day INT NOT NULL,
                    Week INT NOT NULL
                );
            """))

            # Create Fact Tables
            logging.info("Creating fact_daily_stock table.")
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS "STG".fact_daily_stock (
                    RecordID SERIAL PRIMARY KEY,
                    CompanyID INT NOT NULL,
                    DateKey DATE NOT NULL,
                    OpenPrice FLOAT NOT NULL,
                    High FLOAT NOT NULL,
                    Low FLOAT NOT NULL,
                    ClosePrice FLOAT NOT NULL,
                    Volume INT NOT NULL,
                    FOREIGN KEY (CompanyID) REFERENCES "EDW".dim_company (CompanyID),
                    FOREIGN KEY (DateKey) REFERENCES "EDW".dim_date (DateKey)
                );
            """))

            logging.info("Creating fact_stock_aggregate table.")
            connection.execute(text("""
                CREATE TABLE IF NOT EXISTS "EDW".fact_stock_aggregate (
                    RecordID SERIAL PRIMARY KEY,
                    CompanyID INT NOT NULL,
                    DateKey DATE NOT NULL,
                    AverageDailyPrice FLOAT NOT NULL,
                    TotalVolume BIGINT NOT NULL,
                    DailyReturn FLOAT NOT NULL,
                    FOREIGN KEY (CompanyID) REFERENCES "EDW".dim_company (CompanyID),
                    FOREIGN KEY (DateKey) REFERENCES "EDW".dim_date (DateKey)
                );
            """))

            logging.info("Dimension and fact tables are ready.")

    except Exception as e:
        logging.error(f"Error creating tables: {e}")
        raise


def populate_dim_company(data, engine):
    """
    Populates the dim_company table with unique company data.
    """
    try:
        with engine.begin() as connection:
            unique_companies = data[['Symbol', 'CompanyName']].drop_duplicates()
            logging.info(f"Inserting {len(unique_companies)} unique companies into dim_company.")
            
            for _, row in unique_companies.iterrows():
                connection.execute(text("""
                    INSERT INTO "EDW".dim_company (Symbol, CompanyName)
                    VALUES (:Symbol, :CompanyName)
                    ON CONFLICT (Symbol) DO NOTHING;
                """), {"Symbol": row['Symbol'], "CompanyName": row['CompanyName']})
    except Exception as e:
        logging.error(f"Error populating dim_company: {e}")
        raise


def populate_dim_date(data, engine):
    """
    Populates the dim_date table with unique dates and their attributes.
    """
    try:
        with engine.begin() as connection:
            unique_dates = pd.to_datetime(data['DateID']).drop_duplicates()
            logging.info(f"Inserting {len(unique_dates)} unique dates into dim_date.")
            
            for date in unique_dates:
                connection.execute(text("""
                    INSERT INTO "EDW".dim_date (DateID, Year, Month, Day, Week)
                    VALUES (:DateID, :Year, :Month, :Day, :Week)
                    ON CONFLICT (DateID) DO NOTHING;
                """), {
                    "DateID": date,
                    "Year": date.year,
                    "Month": date.month,
                    "Day": date.day,
                    "Week": date.isocalendar()[1]
                })
    except Exception as e:
        logging.error(f"Error populating dim_date: {e}")
        raise


# Function to get the latest file based on naming convention in a specific folder

def get_latest_file(directory, prefix):
    """
    Finds the latest file in the specified directory based on the given prefix.
    """
    try:
        search_pattern = os.path.join(directory, f"{prefix}_*.csv")
        files = glob.glob(search_pattern)

        if not files:
            logging.error(f"No files found with prefix {prefix} in {directory}.")

            return None

        latest_file = max(files, key=os.path.getmtime)
        logging.info(f"Latest file found: {latest_file}")

        return latest_file
    
    except Exception as e:
        logging.error(f"Error finding latest file: {e}")

        raise

def load_data_into_staging(data, table_name, engine):
    """
    Load data into the specified staging table.
    """
    try:
        logging.info(f"Loading data into staging table: {table_name}.")
        row_count = len(data)
        data.to_sql(table_name, con=engine, schema="STG", if_exists='append', index=False)
        logging.info(f"{row_count} rows loaded into {table_name} successfully.")
    except Exception as e:
        logging.error(f"Error loading data into {table_name}: {e}")
        raise


def load_data_into_production(data, table_name, engine):
    """
    Load data into the specified production table.
    """
    try:
        logging.info(f"Loading data into production table: {table_name}.")
        row_count = len(data)
        data.to_sql(table_name, con=engine, schema="EDW", if_exists='append', index=False)
        logging.info(f"{row_count} rows loaded into {table_name} successfully.")
    except Exception as e:
        logging.error(f"Error loading data into {table_name}: {e}")
        raise


def execute_stored_procedure(engine):
    """
    Executes the stored procedure to move data from staging to production.
    """
    try:
        with engine.connect() as connection:
            logging.info("Executing stored procedure to move data from staging to production.")
            connection.execute(text('CALL "STG".agg_apidata();'))
            logging.info("Stored procedure executed successfully.")
    except Exception as e:
        logging.error(f"Error executing stored procedure: {e}")
        raise


def main():
    try:
        from dotenv import load_dotenv
        load_dotenv()
        user = os.getenv("pg_user")
        password = os.getenv("pg_pword")
        host = os.getenv("pg_host")
        port = os.getenv("pg_port")
        dbname = os.getenv("pg_DB")

        DB_URI = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        engine = create_engine(DB_URI)

        create_dimension_and_fact_tables(engine)

        transformed_directory = "Dataset/cleaned_data"
        aggregated_directory = "Dataset/aggregated_data"

        latest_transformed_file = get_latest_file(transformed_directory, "transformed_data")
        if not latest_transformed_file:
            raise FileNotFoundError("No transformed data file found.")
        
        transformed_data = pd.read_csv(latest_transformed_file)
        logging.info(f"Transformed data loaded from {latest_transformed_file}.")

        # Populate Dimension Tables
        populate_dim_company(transformed_data, engine)
        populate_dim_date(transformed_data, engine)

        # Load transformed data into staging
        load_data_into_staging(transformed_data, 'stg.fact_daily_stock', engine)

        latest_aggregated_file = get_latest_file(aggregated_directory, "aggregated_data")
        if not latest_aggregated_file:
            raise FileNotFoundError("No aggregated data file found.")

        aggregated_data = pd.read_csv(latest_aggregated_file)
        logging.info(f"Aggregated data loaded from {latest_aggregated_file}.")

        # Load aggregated data into production
        load_data_into_production(aggregated_data, 'edw.fact_stock_aggregate', engine)

        execute_stored_procedure(engine)



        logging.info("ETL Load process completed successfully.")
    except FileNotFoundError as e:
        logging.error(f"File not found: {e}")
    except OperationalError as e:
        logging.error(f"Database connection error: {e}")
    except ProgrammingError as e:
        logging.error(f"SQL error: {e}")
    except Exception as e:
        logging.critical(f"Critical error in ETL Load process: {e}")
        raise


if __name__ == "__main__":
    main()
