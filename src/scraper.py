import time
import json
import re
import time

from bs4 import BeautifulSoup

PAGE_URL_SUFFIX = '-pagina-'
HTML_EXTENSION = '.html'

class Scraper:
    def __init__(self, browser, base_url):
        self.browser = browser
        self.base_url = base_url

    def scrap_page(self, page_number):
        if page_number == 1:
            page_url = f'{self.base_url}{HTML_EXTENSION}'
        else:
            page_url = f'{self.base_url}{PAGE_URL_SUFFIX}{page_number}{HTML_EXTENSION}'

        print(f'URL: {page_url}')

        page = self.browser.get_text(page_url)

        soup = BeautifulSoup(page, 'lxml')
        estate_posts = soup.find('script', {'id': 'preloadedData'})
        json_str = estate_posts.string
        json_str = json_str.replace("window.__PRELOADED_STATE__ = ", "")
        json_str = json_str.split(";\n\t\t\twindow.__SITE_DATA__")[0]

        data = json.loads(json_str)
        data = data["listStore"]["listPostings"]

        return data


    def scrap_website(self):
        page_number = 1
        estates = []
        estates_scraped = 0
        estates_quantity = self.get_estates_quantity()
        while estates_quantity > estates_scraped:
            print(f'Page: {page_number}')
            estates += self.scrap_page(page_number)
            page_number += 1
            estates_scraped = len(estates)
            time.sleep(3)

        return estates


    def get_estates_quantity(self):
        page_url = f'{self.base_url}{HTML_EXTENSION}'
        page = self.browser.get_text(page_url)
        soup = BeautifulSoup(page, 'lxml')
        text = soup.find_all('h1')[0].text
        words = text.split(" ")
        for word in words:
            try:
                float(word)
                estates_quantity = word.replace('.', '')
                break
            except ValueError:
                pass
        else:
            estates_quantity = None

        estates_quantity = int(estates_quantity)
        return estates_quantity
