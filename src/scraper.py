import time
import json
import re
import random
from statistics import mean
from tqdm import tqdm

from bs4 import BeautifulSoup

PAGE_URL_SUFFIX = '-pagina-'
HTML_EXTENSION = '.html'

class Scraper:
    def __init__(self, browser, base_url):
        self.browser = browser
        self.base_url = base_url
        # Initial sleep times - more aggressive
        self.min_sleep = 0.5  # Start with 500ms minimum
        self.max_sleep = 1.0  # Start with 1s maximum
        self.response_times = []  # Track response times
        self.consecutive_errors = 0  # Track consecutive errors
        self.max_consecutive_errors = 3  # Maximum errors before backing off
        self.backoff_factor = 1.5  # How much to increase sleep time when backing off

    def _get_sleep_time(self):
        """Get an adaptive sleep time based on response times and error rate."""
        if self.consecutive_errors >= self.max_consecutive_errors:
            # Back off if we're getting too many errors
            self.min_sleep *= self.backoff_factor
            self.max_sleep *= self.backoff_factor
            self.consecutive_errors = 0
            print(f"Backing off - new sleep range: {self.min_sleep:.2f}s to {self.max_sleep:.2f}s")
        
        # If we have response time data, use it to adjust sleep time
        if self.response_times:
            avg_response = mean(self.response_times[-5:])  # Use last 5 responses
            # Sleep time should be proportional to response time
            target_sleep = avg_response * 0.5  # Sleep for half the response time
            self.min_sleep = max(0.5, min(self.min_sleep, target_sleep))
            self.max_sleep = max(1.0, min(self.max_sleep, target_sleep * 1.5))
        
        return random.uniform(self.min_sleep, self.max_sleep)

    def _record_response_time(self, start_time):
        """Record the response time for a request."""
        response_time = time.time() - start_time
        self.response_times.append(response_time)
        # Keep only last 20 response times
        if len(self.response_times) > 20:
            self.response_times.pop(0)

    def scrape_page(self, page_number):
        if page_number == 1:
            page_url = f'{self.base_url}{HTML_EXTENSION}'
        else:
            page_url = f'{self.base_url}{PAGE_URL_SUFFIX}{page_number}{HTML_EXTENSION}'

        start_time = time.time()
        page = self.browser.get_text(page_url)
        self._record_response_time(start_time)
        
        soup = BeautifulSoup(page, 'lxml')
        estate_posts = soup.find('script', {'id': 'preloadedData'})
        json_str = estate_posts.string
        json_str = json_str.replace("window.__PRELOADED_STATE__ = ", "")
        json_str = json_str.split(";\n\t\t\twindow.__SITE_DATA__")[0]

        data = json.loads(json_str)
        data = data["listStore"]["listPostings"]

        return data

    def scrape_website(self, first_page_data=None, total_estates=None):
        page_number = 1
        estates = []
        estates_scraped = 0
        
        if first_page_data is None:
            # Get first page and extract total count
            first_page_data = self.scrape_page(page_number)
            estates += first_page_data
            
            # Get total count from the first page's data
            page = self.browser.get_text(f'{self.base_url}{HTML_EXTENSION}')
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
                total_estates = len(first_page_data)  # Fallback to first page count if we can't find total
        else:
            estates += first_page_data
        
        estates_scraped = len(estates)
        
        # Calculate total pages
        total_pages = (total_estates + len(first_page_data) - 1) // len(first_page_data)
        
        # Create progress bar
        with tqdm(total=total_pages, desc="Scraping pages", unit="page", ncols=100) as pbar:
            pbar.update(1)  # Update for first page
            
            # Continue with remaining pages
            while total_estates > estates_scraped:
                page_number += 1
                estates += self.scrape_page(page_number)
                estates_scraped = len(estates)
                time.sleep(self._get_sleep_time())
                
                # Update progress bar every 5 pages
                if page_number % 5 == 0:
                    pbar.update(5)
                elif page_number == total_pages:  # Update remaining pages at the end
                    remaining = total_pages - pbar.n
                    if remaining > 0:
                        pbar.update(remaining)

        return estates
