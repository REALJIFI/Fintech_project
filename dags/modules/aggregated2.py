

import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime
from log import logging


def get_latest_file(directory, prefix="transformed_data", extension=".csv"):

    """
    Get the latest file from a directory based on the modified timestamp.
    """
    try:
        files = [f for f in os.listdir(directory) if f.startswith(prefix) and f.endswith(extension)]
        if not files:
            raise FileNotFoundError(f"No files with prefix '{prefix}' and extension '{extension}' found in {directory}")
        
        latest_file = max(
            files,
            key=lambda x: os.path.getmtime(os.path.join(directory, x))
        )
        logging.info(f"Latest file selected: {latest_file}")
        return os.path.join(directory, latest_file)
    
    except Exception as e:
        logging.error(f"Error finding the latest file: {e}")

        raise


def save_with_timestamp(df, output_directory, prefix="aggregated_data"):

    """
    Save the DataFrame to a CSV file with a timestamped filename.

    """
    
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{prefix}_{timestamp}.csv"
        file_path = os.path.join(output_directory, filename)
        df.to_csv(file_path, index=False)

        logging.info(f"Aggregated data saved to: {file_path}")
        return file_path
    
    except Exception as e:

        logging.error(f"Error saving file: {e}")
        raise


def aggregate_data(transformed_data):

    logging.info(f"Starting aggregation process with {len(transformed_data)} rows")

    try:

        # Handle missing data (fill NaN values with 0 or appropriate placeholder)

        missing_data_count = transformed_data.isna().sum().sum()
        if missing_data_count > 0:
            logging.warning(f"Found {missing_data_count} missing values in the data. Filling missing values where necessary.")
        # transformed_data.fillna(0, inplace=True)  # Adjust based on what is appropriate for your data
        transformed_data['DailyReturn'] = transformed_data['DailyReturn'].fillna(0)


        # 1. Average Daily Price (OpenPrice + ClosePrice) / 2
        transformed_data['AverageDailyPrice'] = (transformed_data['OpenPrice'] + transformed_data['ClosePrice']) / 2
        company_performance_summary = transformed_data.groupby('CompanyID').agg(
            AverageDailyPrice=('AverageDailyPrice', 'mean'),
            TotalVolume=('Volume', 'sum'),
            DailyReturn=('DailyReturn', 'mean')
        ).reset_index()

        logging.info(f"Company Performance Summary: {len(company_performance_summary)} rows")


        # 2. Total Trading Volume (Sum of Volume per period)

        total_trading_volume = transformed_data.groupby('CompanyID').agg(
            TotalTradingVolume=('Volume', 'sum')
        ).reset_index()
        logging.info(f"Total Trading Volume: {len(total_trading_volume)} rows")


        # 3. Daily Returns (Calculated as (ClosePrice - PreviousDayClosePrice) / PreviousDayClosePrice)
        transformed_data['PreviousClosePrice'] = transformed_data.groupby('CompanyID')['ClosePrice'].shift(1)
        transformed_data['DailyReturn'] = (transformed_data['ClosePrice'] - transformed_data['PreviousClosePrice']) / transformed_data['PreviousClosePrice']
        

        # Handle missing daily returns (NaN values for the first row or rows with missing data)
        transformed_data['DailyReturn'].fillna(0, inplace=True)  # Filling NaNs with 0 or another appropriate value
        daily_returns = transformed_data.groupby('CompanyID').agg(
            DailyReturn=('DailyReturn', 'mean')
        ).reset_index()
        logging.info(f"Daily Returns: {len(daily_returns)} rows")


        # 4. Volatility Index (Standard deviation of daily returns)
        volatility_index = transformed_data.groupby('CompanyID').agg(
            VolatilityIndex=('DailyReturn', 'std')
        ).reset_index()
        logging.info(f"Volatility Index: {len(volatility_index)} rows")


        # 5. Moving Averages (5-day and 10-day moving averages of ClosePrice)
        transformed_data['5DayMovingAvg'] = transformed_data.groupby('CompanyID')['ClosePrice'].transform(lambda x: x.rolling(window=5).mean())
        transformed_data['10DayMovingAvg'] = transformed_data.groupby('CompanyID')['ClosePrice'].transform(lambda x: x.rolling(window=10).mean())
        

        # Handle NaNs in moving averages
        #transformed_data['5DayMovingAvg'].fillna(0, inplace=True)
        #transformed_data['10DayMovingAvg'].fillna(0, inplace=True)
        transformed_data['5DayMovingAvg'] = transformed_data['5DayMovingAvg'].fillna(0)
        transformed_data['10DayMovingAvg'] = transformed_data['10DayMovingAvg'].fillna(0)


        moving_averages = transformed_data.groupby('CompanyID').agg(
            MovingAvg5Day=('5DayMovingAvg', 'last'),  # Taking the last value in the window as the final moving average
            MovingAvg10Day=('10DayMovingAvg', 'last')  # Same for 10-day moving average
        ).reset_index()
        logging.info(f"Moving Averages: {len(moving_averages)} rows")


        # 6. Trend Analysis: Percentage Change in Closing Prices (weekly/monthly trend)
        transformed_data['Trend'] = transformed_data.groupby('CompanyID')['ClosePrice'].pct_change(periods=5) * 100  # 5-day trend
        # transformed_data['Trend'].fillna(0, inplace=True)  # Fill NaNs in trend calculation with 0 (or appropriate value)
        transformed_data['Trend'] = transformed_data['Trend'].fillna(0)

        trend_analysis = transformed_data.groupby('CompanyID').agg(
            TrendAnalysis=('Trend', 'mean')
        ).reset_index()
        logging.info(f"Trend Analysis: {len(trend_analysis)} rows")


        # 7. High/Low Ratios (HighPrice / LowPrice)
        transformed_data['HighLowRatio'] = transformed_data['High'] / transformed_data['Low']
        transformed_data['HighLowRatio'].fillna(0, inplace=True)  # Fill NaNs in ratio calculation
        high_low_ratios = transformed_data.groupby('CompanyID').agg(
            HighLowRatio=('HighLowRatio', 'mean')
        ).reset_index()
        logging.info(f"High/Low Ratios: {len(high_low_ratios)} rows")


        # 8. Volume Analysis: Average Volume
        volume_analysis = transformed_data.groupby('CompanyID').agg(
            AverageVolume=('Volume', 'mean')
        ).reset_index()
        logging.info(f"Volume Analysis: {len(volume_analysis)} rows")


        # Combine all aggregation results into a single dataframe
        aggregated_data = company_performance_summary.merge(total_trading_volume, on='CompanyID', how='left')
        aggregated_data = aggregated_data.merge(daily_returns, on='CompanyID', how='left')
        aggregated_data = aggregated_data.merge(volatility_index, on='CompanyID', how='left')
        aggregated_data = aggregated_data.merge(moving_averages, on='CompanyID', how='left')
        aggregated_data = aggregated_data.merge(trend_analysis, on='CompanyID', how='left')
        aggregated_data = aggregated_data.merge(high_low_ratios, on='CompanyID', how='left')
        aggregated_data = aggregated_data.merge(volume_analysis, on='CompanyID', how='left')

        logging.info(f"Aggregation complete. Total aggregated data: {len(aggregated_data)} rows")
        
        return aggregated_data

    except Exception as e:
        logging.error(f"Error during aggregation: {e}")
        raise


def main(input_directory, output_directory):

    try:
        # Step 1: Get the latest transformed data file
        latest_file_path = get_latest_file(input_directory, prefix="transformed_data")
        transformed_data = pd.read_csv(latest_file_path)
        logging.info(f"Loaded transformed data from: {latest_file_path}")

        # Step 2: Perform aggregation
        aggregated_data = aggregate_data(transformed_data)

        # Step 3: Save aggregated data with a timestamped filename
        save_with_timestamp(aggregated_data, output_directory)
    except Exception as e:
        logging.error(f"Error in the main aggregation process: {e}")


# Example Usage

if __name__ == "__main__":

    input_file = "Dataset/cleaned_data"  # Replace with your directory path
    output_file = "Dataset/aggregated_data"  # Replace with your directory path

    main(input_file, output_file)