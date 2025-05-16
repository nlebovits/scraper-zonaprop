import time
import argparse
from typing import List

import pandas as pd
from bs4 import BeautifulSoup

from src import utils
from src.browser import Browser
from src.scraper import Scraper

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Define valid property and transaction types
PROPERTY_TYPES = ["departamentos", "casas", "terrenos", "locales-comerciales", "ph"]
TRANSACTION_TYPES = ["venta", "alquiler"]


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
) -> None:
    """
    Main function to scrape real estate data from ZonaProp.

    Args:
        url: Optional direct URL of the ZonaProp search page to scrape
        property_types: List of property types to scrape (departamentos, casas, terrenos, etc.)
        transaction_type: Type of transaction (venta or alquiler)
        limit: Optional limit on the number of results to scrape (will be split evenly across property types)
    """
    start_time = time.time()
    all_estates = []

    # If URL is provided, just scrape that URL
    if url is not None:
        base_url = utils.parse_zonaprop_url(url)
        logging.info(f"Starting scraper for {base_url}")
        browser = Browser()
        scraper = Scraper(browser, base_url)

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
            logging.info(f"Limited to {total_estates:,} estates")

        logging.info(f"Found {total_estates:,} total estates to scrape")

        estates = scraper.scrape_website(
            first_page_data=first_page_data, total_estates=total_estates
        )
        all_estates.extend(estates)
        base_url_for_save = base_url

    # If property types are provided, scrape each one
    elif property_types is not None:
        browser = Browser()
        # Calculate limit per property type
        limit_per_type = limit // len(property_types) if limit is not None else None
        if limit is not None:
            logging.info(
                f"Limit of {limit:,} will be split into {limit_per_type:,} per property type"
            )

        # Create a combined base URL for saving
        base_url_for_save = (
            f"https://www.zonaprop.com.ar/{'-'.join(property_types)}-{transaction_type}"
        )

        for prop_type in property_types:
            base_url = f"https://www.zonaprop.com.ar/{prop_type}-{transaction_type}"
            logging.info(f"Starting scraper for {base_url}")
            scraper = Scraper(browser, base_url)

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

            # Apply limit per type if specified
            if limit_per_type is not None:
                total_estates = min(total_estates, limit_per_type)
                logging.info(f"Limited to {total_estates:,} estates for {prop_type}")

            logging.info(
                f"Found {total_estates:,} total estates to scrape for {prop_type}"
            )

            estates = scraper.scrape_website(
                first_page_data=first_page_data, total_estates=total_estates
            )
            all_estates.extend(estates)

            # Add a small delay between different property types
            time.sleep(2)

    else:
        raise ValueError("Either url or property_types must be provided")

    logging.info("Scraping finished. Processing data...")

    # Process all estates at once instead of row by row
    flattened_estates = [utils.flatten_json(estate) for estate in all_estates]
    df = pd.DataFrame(flattened_estates)

    logging.info("Saving data to CSV files")
    utils.save_df_to_csv(df, base_url_for_save)

    utils.monitoring(df, start_time)


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

    args = parser.parse_args()

    # Validate that either URL or property types are provided
    if not args.url and not args.property_types:
        parser.error("Either --url or --property-types must be provided")

    main(
        url=args.url,
        property_types=args.property_types,
        transaction_type=args.transaction_type,
        limit=args.limit,
    )


# https://www.zonaprop.com.ar/terrenos-venta-capital-federal-gba-norte-gba-sur-gba-oeste.html
# https://www.zonaprop.com.ar/departamentos-alquiler.html
# https://www.zonaprop.com.ar/departamentos-venta.html
# https://www.zonaprop.com.ar/casas-venta.html
# https://www.zonaprop.com.ar/terrenos-venta.html
# https://www.zonaprop.com.ar/ph-venta.html
# https://www.zonaprop.com.ar/locales-comerciales-venta.html
