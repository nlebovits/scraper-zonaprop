import time

import pandas as pd

from src import utils
from src.browser import Browser
from src.scraper import Scraper

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def main(url: str) -> None:
    """
    Main function to scrape real estate data from ZonaProp.
    
    Args:
        url: The base URL of the ZonaProp search page to scrape
    """
    start_time = time.time()
    base_url = utils.parse_zonaprop_url(url)

    logging.info(f'Starting scraper for {base_url}')
    browser = Browser()
    scraper = Scraper(browser, base_url)
    
    # Get total number of estates before starting
    total_estates = scraper.get_estates_quantity()
    logging.info(f'Found {total_estates:,} total estates to scrape')
    
    # Get number of estates on first page to calculate total pages
    first_page_estates = len(scraper.scrap_page(1))
    total_pages = (
        (total_estates + first_page_estates - 1) // first_page_estates
    )
    logging.info(
        f'Will scrape {total_pages:,} pages '
        f'({first_page_estates} estates per page)'
    )
    
    estates = scraper.scrap_website()
    
    logging.info('Scraping finished. Processing data...')
    df = pd.DataFrame(estates)
    df = df.apply(lambda x: utils.flatten_json(x.to_dict()), axis=1)
    df = pd.DataFrame(df.tolist())

    logging.info('Saving data to CSV files')
    utils.save_df_to_csv(df, base_url)

    utils.monitoring(df, start_time)


if __name__ == '__main__':
    url = 'https://www.zonaprop.com.ar/departamentos-alquiler.html'
    main(url)