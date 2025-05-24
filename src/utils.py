import datetime
import os
import re
import time
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

# Set up logging to only show warnings and errors
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

columns = {
    "postingId": "posting_id",
    "priceOperationTypes[0].operationType.name": "status",
    "priceOperationTypes[0].prices[0].formattedAmount": "price",
    "priceOperationTypes[0].prices[0].currency": "currency_price",
    "expenses.formattedAmount": "expenses",
    "expenses.currency": "currency_expenses",
    "mainFeatures.CFT100.value": "m2_total",
    "mainFeatures.CFT101.value": "m2_covered",
    "mainFeatures.CFT1.value": "room",
    "mainFeatures.CFT2.value": "bedroom",
    "mainFeatures.CFT3.value": "bathroom",
    "mainFeatures.CFT5.value": "antiquity",
    "mainFeatures.CFT7.value": "garage",
    "publisher.publisherId": "publisher_id",
    "publisher.name": "publisher_name",
    "realEstateType.name": "type",
    "postingLocation.postingGeolocation.geolocation.latitude": "geo_latitude",
    "postingLocation.postingGeolocation.geolocation.longitude": "geo_longitude",
}


def remove_host_from_url(url):
    host_regex = r"(^https?://)(.*/)"
    return re.sub(host_regex, "", url)


def get_filename_from_datetime(base_url, extension):
    base_url_without_host = remove_host_from_url(base_url)
    return f'data/{base_url_without_host}-{datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")}.{extension}'


def save_df_to_csv(df, base_url):
    def save_file(suffix, selected_columns, rename_columns):
        filename = get_filename_from_datetime(base_url + suffix, "csv")
        create_root_directory(filename)
        if selected_columns:
            # Filter out columns that don't exist in the DataFrame
            existing_columns = [col for col in selected_columns if col in df.columns]
            if existing_columns:
                df_selected = df.loc[:, existing_columns].rename(columns=rename_columns)
            else:
                logging.warning(f"No matching columns found for {suffix} file")
                return
        else:
            df_selected = df.copy()
        df_selected.to_csv(filename, index=False)
        logging.info(f"Data saved to {filename}")

    save_file("_COMPLETE", None, None)
    save_file("_PARTIAL", list(columns.keys()), columns)


def parse_zonaprop_url(url):
    return url.replace(".html", "")


def create_root_directory(filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)


def flatten_json(data, prefix=""):
    """
    Flatten a nested JSON structure into a single level dictionary.
    Optimized for the specific structure of ZonaProp data.
    """
    result = {}

    # Handle the most common cases first for better performance
    if not isinstance(data, dict):
        return {prefix[:-1]: data} if prefix else {prefix: data}

    for key, value in data.items():
        new_key = f"{prefix}{key}"

        if isinstance(value, dict):
            # Handle nested dictionaries
            if key in ["priceOperationTypes", "mainFeatures", "postingLocation"]:
                # Special handling for known nested structures
                for subkey, subvalue in value.items():
                    if isinstance(subvalue, dict):
                        for k, v in subvalue.items():
                            result[f"{new_key}.{subkey}.{k}"] = v
                    else:
                        result[f"{new_key}.{subkey}"] = subvalue
            else:
                # Generic nested dictionary handling
                nested = flatten_json(value, f"{new_key}.")
                result.update(nested)
        elif isinstance(value, list):
            # Handle lists (typically only one level deep in our data)
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    for k, v in item.items():
                        result[f"{new_key}[{i}].{k}"] = v
                else:
                    result[f"{new_key}[{i}]"] = item
        else:
            result[new_key] = value

    return result


def get_run_directory(base_url):
    """Get the directory for this run's files."""
    base_url_without_host = remove_host_from_url(base_url)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    run_dir = f'data/{base_url_without_host}-{timestamp}'
    os.makedirs(run_dir, exist_ok=True)
    return run_dir


def save_df_to_parquet(df, base_url, pbar=None):
    """
    Save DataFrame to a Parquet file with timestamp.
    Each script run creates a new directory with timestamp.
    
    Args:
        df: DataFrame to save
        base_url: Base URL for the scrape
        pbar: Optional tqdm progress bar to update
    """
    base_url_without_host = remove_host_from_url(base_url)
    
    # Add timestamp to the data
    df['scraped_at'] = pd.Timestamp.now()
    
    # Create new directory and file
    run_dir = get_run_directory(base_url)
    filename = f'{run_dir}/data.parquet'
    
    try:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, filename)
        if pbar:
            pbar.set_description(f"Saved {len(df):,} properties to {run_dir}")
    except Exception as e:
        # Log detailed error information
        logging.error(f"Error saving to {filename}:")
        logging.error(f"  Error type: {type(e).__name__}")
        logging.error(f"  Error message: {str(e)}")
        logging.error(f"  Data shape: {df.shape}")
        logging.error(f"  Data columns: {df.columns.tolist()}")
        if hasattr(e, '__traceback__'):
            import traceback
            logging.error(f"  Traceback:\n{''.join(traceback.format_tb(e.__traceback__))}")
        raise


def monitoring(df, start_time):
    """Print final statistics about the scraping run."""
    num_rows = df.shape[0]
    end_time = time.time()
    elapsed_time = end_time - start_time
    elapsed_time_formatted = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
    row_rate = elapsed_time / num_rows * 100
    row_rate_formatted = time.strftime("%H:%M:%S", time.gmtime(row_rate))

    print(f"\nScraping completed:")
    print(f"Total properties: {num_rows:,}")
    print(f"Total time: {elapsed_time_formatted}")
    print(f"Rate: 100 properties every {row_rate_formatted}")


def get_latest_parquet_file(base_url):
    """Get the most recent parquet file for a given base URL."""
    base_url_without_host = remove_host_from_url(base_url)
    data_dir = 'data'
    if not os.path.exists(data_dir):
        return None
    
    # Find all matching parquet files
    pattern = f"{base_url_without_host}-*.parquet"
    matching_files = [f for f in os.listdir(data_dir) if f.startswith(base_url_without_host) and f.endswith('.parquet')]
    
    if not matching_files:
        return None
    
    # Sort by timestamp in filename and get the most recent
    latest_file = sorted(matching_files)[-1]
    return os.path.join(data_dir, latest_file)
