import time

import pandas as pd
from bs4 import BeautifulSoup

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
    
    # Get first page to determine total estates and estates per page
    first_page_data = scraper.scrape_page(1)
    first_page_estates = len(first_page_data)
    
    # Get total count from the first page
    page = browser.get_text(f'{base_url}.html')
    soup = BeautifulSoup(page, 'lxml')
    text = soup.find_all('h1')[0].text
    words = text.split(" ")
    for word in words:
        try:
            float(word)
            total_estates = int(word.replace('.', ''))
            break
        except ValueError:
            pass
    else:
        total_estates = first_page_estates  # Fallback to first page count if we can't find total
    
    total_pages = (total_estates + first_page_estates - 1) // first_page_estates
    
    logging.info(f'Found {total_estates:,} total estates to scrape')
    
    estates = scraper.scrape_website(first_page_data=first_page_data, total_estates=total_estates)
    
    logging.info('Scraping finished. Processing data...')
    
    # Process all estates at once instead of row by row
    flattened_estates = [utils.flatten_json(estate) for estate in estates]
    df = pd.DataFrame(flattened_estates)

    logging.info('Saving data to CSV files')
    utils.save_df_to_csv(df, base_url)

    utils.monitoring(df, start_time)


if __name__ == '__main__':
    url = 'https://www.zonaprop.com.ar/terrenos-venta-capital-federal-gba-norte-gba-sur-gba-oeste.html'
    main(url)


# https://www.zonaprop.com.ar/terrenos-venta-capital-federal-gba-norte-gba-sur-gba-oeste.html
# https://www.zonaprop.com.ar/departamentos-alquiler.html