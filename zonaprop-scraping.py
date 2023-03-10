import time

import pandas as pd

from src import utils
from src.browser import Browser
from src.scraper import Scraper

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

def main(url):
    start_time = time.time()
    base_url = utils.parse_zonaprop_url(url)

    logging.info(f'Running scraper for {base_url}')
    browser = Browser()
    scraper = Scraper(browser, base_url)
    estates = scraper.scrap_website()

    logging.info('Scraping finished. Flatten json pending.')
    df = pd.DataFrame(estates)
    df = df.apply(lambda x: utils.flatten_json(x.to_dict()), axis=1)
    df = pd.DataFrame(df.tolist())

    logging.info('Saving data to csv file')
    utils.save_df_to_csv(df, base_url)

    utils.monitoring(df, start_time)

if __name__ == '__main__':
    url = 'https://www.zonaprop.com.ar/inmuebles-alquiler-capital-federal.html'
    main(url)
