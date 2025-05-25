import time
import argparse
from typing import List
import sys
from tqdm import tqdm
import subprocess
import platform
import atexit

import pandas as pd
from bs4 import BeautifulSoup

from src import utils
from src.browser import Browser
from src.scraper import Scraper, BlockedError, ScrapingError

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Define valid property and transaction types
PROPERTY_TYPES = ["departamentos", "casas", "terrenos", "locales-comerciales", "ph"]
TRANSACTION_TYPES = ["venta", "alquiler"]


def prevent_system_sleep():
    """Prevent system from sleeping during scraping."""
    system = platform.system().lower()
    process = None
    
    try:
        if system == "darwin":  # macOS
            process = subprocess.Popen(["caffeinate", "-i"])
            print("System sleep prevention enabled (macOS)")
        elif system == "linux":
            process = subprocess.Popen(["systemd-inhibit", "--what=sleep", "--who=ZonaProp Scraper", "--why=Scraping in progress", "sleep", "infinity"])
            print("System sleep prevention enabled (Linux)")
        else:
            print("Warning: System sleep prevention not supported on this platform")
    except Exception as e:
        print(f"Warning: Could not prevent system sleep: {e}")
    
    def cleanup():
        if process:
            process.terminate()
            print("System sleep prevention disabled")
    
    atexit.register(cleanup)
    return process


def build_url(property_types: List[str], transaction_type: str) -> str:
    """
    Build the ZonaProp URL based on property types and transaction type.

    Args:
        property_types: List of property types to scrape
        transaction_type: Type of transaction (venta or alquiler)

    Returns:
        str: Complete ZonaProp URL
    """
    if len(property_types) == 1:
        return (
            f"https://www.zonaprop.com.ar/{property_types[0]}-{transaction_type}.html"
        )
    else:
        # For multiple property types, we need to use the search URL format
        # Example: https://www.zonaprop.com.ar/terrenos-venta-capital-federal-gba-norte-gba-sur-gba-oeste.html
        # We'll use the first property type as the base and add the others as filters
        base_type = property_types[0]
        # Convert additional types to their search format
        additional_types = []
        for prop_type in property_types[1:]:
            if prop_type == "ph":
                additional_types.append("ph")
            elif prop_type == "locales-comerciales":
                additional_types.append("locales")
            else:
                additional_types.append(prop_type)

        # Join with hyphens and add to the base URL
        additional_str = "-".join(additional_types)
        return f"https://www.zonaprop.com.ar/{base_type}-{transaction_type}-{additional_str}.html"


def main(
    url: str = None,
    property_types: List[str] = None,
    transaction_type: str = "venta",
    limit: int = None,
    num_batches: int = 1,
) -> None:
    """
    Main function to scrape real estate data from ZonaProp.

    Args:
        url: Optional direct URL of the ZonaProp search page to scrape
        property_types: List of property types to scrape (departamentos, casas, terrenos, etc.)
        transaction_type: Type of transaction (venta or alquiler)
        limit: Optional limit on the number of results to scrape (will be split evenly across property types)
        num_batches: Number of batches to split the data into (default: 1, meaning all data in memory)
    """
    # Prevent system sleep
    sleep_prevention = prevent_system_sleep()
    
    start_time = time.time()
    all_estates = []
    total_pages = 0
    processed_pages = 0
    retry_count = 0
    max_retries = 1

    def save_intermediate_results(base_url, pbar):
        if len(all_estates) > 0:
            print("\nSaving intermediate results before exiting...")
            flattened_estates = [utils.flatten_json(estate) for estate in all_estates]
            df = pd.DataFrame(flattened_estates)
            utils.save_df_to_parquet(df, base_url, pbar)
            print(f"Saved {len(all_estates):,} properties before exiting")

    def handle_error(error, base_url, pbar):
        nonlocal retry_count
        save_intermediate_results(base_url, pbar)
        
        if retry_count < max_retries:
            retry_count += 1
            print(f"\nError occurred. Waiting 5 minutes before retry {retry_count}/{max_retries}...")
            time.sleep(300)  # 5 minutes
            return True
        else:
            if isinstance(error, BlockedError):
                print("Scraping was blocked. Please follow the suggestions above and try again.")
            else:
                print("Scraping failed. Please check the error message above.")
            sys.exit(1)
        return False

    # If URL is provided, just scrape that URL
    if url is not None:
        base_url = utils.parse_zonaprop_url(url)
        print(f"Starting scraper for {base_url}")
        browser = Browser()
        scraper = Scraper(browser, base_url)

        while True:
            try:
                # Get first page to determine total estates and estates per page
                first_page_data = scraper.scrape_page(1)
                first_page_estates = len(first_page_data)

                # Get total count from the first page
                page = browser.get_text(f"{base_url}.html")
                soup = BeautifulSoup(page, "lxml")
                text = soup.find_all("h1")[0].text
                words = text.split(" ")
                for word in words:
                    try:
                        float(word)
                        total_estates = int(word.replace(".", ""))
                        break
                    except ValueError:
                        pass
                else:
                    total_estates = first_page_estates  # Fallback to first page count if we can't find total

                # Apply limit if specified
                if limit is not None:
                    total_estates = min(total_estates, limit)
                    print(f"Limited to {total_estates:,} properties")

                print(f"Found {total_estates:,} properties to scrape")

                # Calculate total pages
                total_pages = (total_estates + first_page_estates - 1) // first_page_estates
                print(f"Will process {total_pages} pages")
                
                # Create progress bar for total pages
                pbar = tqdm(total=total_pages, desc="Scraping pages", unit="page")
                
                # Add first page data
                all_estates.extend(first_page_data)
                processed_pages += 1
                pbar.update(1)
                
                # Process remaining pages
                for page_num in range(2, total_pages + 1):
                    try:
                        page_data = scraper.scrape_page(page_num)
                        all_estates.extend(page_data)
                        time.sleep(scraper._get_sleep_time())
                        processed_pages += 1
                        pbar.update(1)
                        
                    except (BlockedError, Exception) as e:
                        if not handle_error(e, base_url, pbar):
                            raise

                pbar.close()
                break  # Success, exit the retry loop

            except (BlockedError, Exception) as e:
                if not handle_error(e, base_url, pbar):
                    raise

        # Save all data
        if num_batches == 1:
            # Single batch - save all data at once
            flattened_estates = [utils.flatten_json(estate) for estate in all_estates]
            df = pd.DataFrame(flattened_estates)
            utils.save_df_to_parquet(df, base_url, pbar)
        else:
            # Multiple batches
            batch_size = len(all_estates) // num_batches
            for i in range(num_batches):
                start_idx = i * batch_size
                end_idx = start_idx + batch_size if i < num_batches - 1 else len(all_estates)
                batch_estates = all_estates[start_idx:end_idx]
                flattened_estates = [utils.flatten_json(estate) for estate in batch_estates]
                df = pd.DataFrame(flattened_estates)
                utils.save_df_to_parquet(df, base_url, pbar)

        base_url_for_save = base_url

    # If property types are provided, scrape each one
    elif property_types is not None:
        browser = Browser()
        # Calculate limit per property type
        limit_per_type = limit // len(property_types) if limit is not None else None
        if limit is not None:
            print(
                f"Limit of {limit:,} properties will be split into {limit_per_type:,} properties per type across {len(property_types)} property types"
            )

        # Create a combined base URL for saving
        base_url_for_save = (
            f"https://www.zonaprop.com.ar/{'-'.join(property_types)}-{transaction_type}"
        )

        for prop_type in property_types:
            base_url = f"https://www.zonaprop.com.ar/{prop_type}-{transaction_type}"
            print(f"Starting scraper for {base_url}")
            scraper = Scraper(browser, base_url)

            while True:
                try:
                    # Get first page to determine total estates and estates per page
                    first_page_data = scraper.scrape_page(1)
                    first_page_estates = len(first_page_data)

                    # Get total count from the first page
                    page = browser.get_text(f"{base_url}.html")
                    soup = BeautifulSoup(page, "lxml")
                    text = soup.find_all("h1")[0].text
                    words = text.split(" ")
                    for word in words:
                        try:
                            float(word)
                            total_estates = int(word.replace(".", ""))
                            break
                        except ValueError:
                            pass
                    else:
                        total_estates = first_page_estates

                    # Apply limit per type if specified
                    if limit_per_type is not None:
                        total_estates = min(total_estates, limit_per_type)
                        print(f"Limited to {total_estates:,} {prop_type} properties")

                    print(f"Found {total_estates:,} {prop_type} properties to scrape")

                    # Calculate total pages
                    total_pages = (total_estates + first_page_estates - 1) // first_page_estates
                    print(f"Will process {total_pages} pages")
                    
                    # Create progress bar for total pages
                    pbar = tqdm(total=total_pages, desc=f"Scraping {prop_type} pages", unit="page")
                    
                    # Add first page data
                    all_estates.extend(first_page_data)
                    processed_pages += 1
                    pbar.update(1)
                    
                    # Process remaining pages
                    for page_num in range(2, total_pages + 1):
                        try:
                            page_data = scraper.scrape_page(page_num)
                            all_estates.extend(page_data)
                            time.sleep(scraper._get_sleep_time())
                            processed_pages += 1
                            pbar.update(1)
                            
                        except (BlockedError, Exception) as e:
                            if not handle_error(e, base_url_for_save, pbar):
                                raise

                    pbar.close()
                    break  # Success, exit the retry loop

                except (BlockedError, Exception) as e:
                    if not handle_error(e, base_url_for_save, pbar):
                        raise

                # Add a small delay between different property types
                time.sleep(2)

    else:
        raise ValueError("Either url or property_types must be provided")

    print("Scraping finished. Processing final data...")
    # Convert final list to DataFrame for monitoring
    final_df = pd.DataFrame([utils.flatten_json(estate) for estate in all_estates])
    utils.monitoring(final_df, start_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape real estate data from ZonaProp"
    )
    parser.add_argument(
        "--url",
        help="Direct URL to scrape (if not using property types)",
    )
    parser.add_argument(
        "--property-types",
        "-p",
        nargs="+",
        choices=PROPERTY_TYPES,
        help="Property types to scrape (can specify multiple)",
    )
    parser.add_argument(
        "--transaction-type",
        "-t",
        choices=TRANSACTION_TYPES,
        default="venta",
        help="Type of transaction (venta or alquiler)",
    )
    parser.add_argument(
        "--limit", "-l", type=int, help="Limit the number of results to scrape"
    )
    parser.add_argument(
        "--num-batches", "-b", type=int, default=1,
        help="Number of batches to split the data into (default: 1, meaning all data in memory)"
    )

    args = parser.parse_args()

    # Validate that either URL or property types are provided
    if not args.url and not args.property_types:
        parser.error("Either --url or --property-types must be provided")

    main(
        url=args.url,
        property_types=args.property_types,
        transaction_type=args.transaction_type,
        limit=args.limit,
        num_batches=args.num_batches,
    )


# https://www.zonaprop.com.ar/terrenos-venta-capital-federal-gba-norte-gba-sur-gba-oeste.html
# https://www.zonaprop.com.ar/departamentos-alquiler.html
# https://www.zonaprop.com.ar/departamentos-venta.html
# https://www.zonaprop.com.ar/casas-venta.html
# https://www.zonaprop.com.ar/terrenos-venta.html
# https://www.zonaprop.com.ar/ph-venta.html
# https://www.zonaprop.com.ar/locales-comerciales-venta.html
