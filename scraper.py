import logging
import csv
from concurrent.futures import ThreadPoolExecutor
import requests
from bs4 import BeautifulSoup
from string import Template
import time
from datetime import datetime
import re

logging.basicConfig(level=logging.INFO)  
logger = logging.getLogger(__name__)

HEADERS = ({'User-Agent':
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
            'Accept-Language': 'en-US, en;q=0.5'})


class EbayScraper:
    def __init__(self, query):
        self.query = query
        self.results = []
        self.num_products = 1
        self.query_template = Template("https://www.ebay.com/sch/i.html?_from=R40&_nkw=$query&_sacat=0")

    def get_links(self):
        search_query = self.query_template.substitute(query=self.query)
        try:
            page = requests.get(search_query)
            page.raise_for_status()
            soup = BeautifulSoup(page.text, 'html.parser')
            links = soup.select('div.s-item__info a.s-item__link')
            links = [link.get('href') for link in links]
            logger.info("Search Query Executed: %s", datetime.now().strftime('%H:%M:%S'))
            return links
        except requests.RequestException as e:
            logger.error("Error executing Search Query %s: %s", search_query, e)
            return []

    def get_product_details(self, serial_number, url):
        result = {
            'Serial Number': serial_number,
            'ID': '',
            'Price': '',
            'Title': '',
            'Condition': '',
            'Available': '',
            'Sold': ''
        }

        try:
            page = requests.get(url)
            page.raise_for_status()
            soup = BeautifulSoup(page.text, 'html.parser')
            pattern = re.compile(r"/itm/(\d+)")
            match = pattern.search(url)
            item_id = match.group(1)
            result['ID'] = item_id


            try:
                result['Price'] = soup.select_one('div.x-price-primary span.ux-textspans').get_text()
            except AttributeError as e:
                logger.error("Error extracting price for %s: %s", url, e)
                return

            try:
                result['Title'] = soup.select_one('h1.x-item-title__mainTitle').get_text()
            except AttributeError as e:
                logger.error("Error extracting title for %s: %s", url, e)
                return

            try:
                result['Condition'] = soup.select_one('div.x-item-condition-text span.ux-textspans').get_text()
            except AttributeError as e:
                logger.error("Error extracting condition for %s: %s", url, e)

            try:
                availability_sold = soup.select('div.d-quantity__availability span.ux-textspans')
                availability_sold = [ele.get_text() for ele in availability_sold]
                result['Available'] = availability_sold[0]
                result['Sold'] = availability_sold[2]
            except AttributeError as e:
                logger.error("Error extracting availability/sold for %s: %s", url, e)


        except requests.RequestException as e:
            logger.error("Error processing URL %s: %s", url, e)
        self.num_products+=1
        self.results.append(result)

    def single_threaded_scraper(self):
        links = self.get_links()
        start_time = time.time()
        try:
            for url in links:
                self.get_product_details(self.num_products, url)

        except requests.RequestException as e:
            logger.error("Error executing search query: %s", e)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info("All links parsed: %s", datetime.now().strftime('%H:%M:%S'))
        logger.info("Time taken for single-threaded processing: %.2f seconds", elapsed_time)
        
        
    def multi_threaded_scraper(self, max_threads):
        links = self.get_links()
        start_time = time.time()

        try:
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                for serial_number, url in enumerate(links, start=1):
                    executor.submit(self.get_product_details, serial_number, url)

        except requests.RequestException as e:
            logger.error(f"Error executing search query: {e}")
            
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info("All links parsed: %s", datetime.now().strftime('%H:%M:%S'))
        logger.info("Time taken for multi-threaded processing: %.2f seconds", elapsed_time)

    def write_results_to_csv(self, filename):
        sorted_results = sorted(self.results, key=lambda x: x['Serial Number'])
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = ['Serial Number', 'ID', 'Price', 'Title', 'Condition', 'Available', 'Sold']
            csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            csv_writer.writeheader()

            for result in sorted_results:
                csv_writer.writerow(result)
                
                
class FlipkartScraper:
    def __init__(self, query):
        self.query = query
        self.results = []
        self.num_products = 1
        self.query_template = Template("https://www.flipkart.com/search?q=$query")

    def get_links(self):
        search_query = self.query_template.substitute(query=self.query)
        try:
            page = requests.get(search_query)
            page.raise_for_status()
            soup = BeautifulSoup(page.text, 'html.parser')
            links = soup.select('a.s1Q9rs')
            links = [link.get('href') for link in links]
            links = ["https://www.flipkart.com"+link for link in links]
            logger.info("Search Query Executed: %s", datetime.now().strftime('%H:%M:%S'))
            return links
        except requests.RequestException as e:
            logger.error("Error executing Search Query %s: %s", search_query, e)
            return []

    def get_product_details(self, serial_number, url):
        result = {
            'Serial Number': serial_number,
            'Title': '',
            'Price': '',
            'Rating': '',
            'Offer': ''
        }

        try:
            page = requests.get(url)
            page.raise_for_status()
            soup = BeautifulSoup(page.text, 'html.parser')

            try:
                result['Price'] = soup.select_one('div._16Jk6d').get_text()
            except AttributeError as e:
                logger.error("Error extracting price for %s: %s", url, e)
                return
            try:
                result['Title'] = soup.select_one('span.B_NuCI').get_text()
            except AttributeError as e:
                logger.error("Error extracting title for %s: %s", url, e)
                return

            try:
                result['Rating'] = soup.select_one('div._3LWZlK').get_text()
            except AttributeError as e:
                logger.error("Error extracting condition for %s: %s", url, e)

            try:
                result['Offer'] = soup.select_one('div._3Ay6Sb').get_text()
            except AttributeError as e:
                logger.error("Error extracting condition for %s: %s", url, e)


        except requests.RequestException as e:
            logger.error("Error processing URL %s: %s", url, e)
        self.num_products+=1
        self.results.append(result)

    def single_threaded_scraper(self):
        links = self.get_links()
        start_time = time.time()
        try:
            for url in links:
                self.get_product_details(self.num_products, url)

        except requests.RequestException as e:
            logger.error("Error executing search query: %s", e)
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info("All links parsed: %s", datetime.now().strftime('%H:%M:%S'))
        logger.info("Time taken for single-threaded processing: %.2f seconds", elapsed_time)
        
        
    def multi_threaded_scraper(self, max_threads):
        links = self.get_links()
        start_time = time.time()

        try:
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                for serial_number, url in enumerate(links, start=1):
                    executor.submit(self.get_product_details, serial_number, url)

        except requests.RequestException as e:
            logger.error(f"Error executing search query: {e}")
            
        end_time = time.time()
        elapsed_time = end_time - start_time
        logger.info("All links parsed: %s", datetime.now().strftime('%H:%M:%S'))
        logger.info("Time taken for multi-threaded processing: %.2f seconds", elapsed_time)

    def write_results_to_csv(self, filename):
        sorted_results = sorted(self.results, key=lambda x: x['Serial Number'])
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = ['Serial Number', 'Title', 'Price', 'Rating', 'Offer']
            csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            csv_writer.writeheader()

            for result in sorted_results:
                csv_writer.writerow(result)


if __name__ == "__main__":
    ebay_scraper1 = EbayScraper(query="earphones")
    ebay_scraper2 = EbayScraper(query="laptop")
    ebay_scraper1.single_threaded_scraper()
    ebay_scraper2.multi_threaded_scraper(10)
    ebay_scraper1.write_results_to_csv("earphones.csv")
    ebay_scraper2.write_results_to_csv("laptops.csv")
    
    flipkart_scraper1 = FlipkartScraper(query="earphones")
    flipkart_scraper2 = FlipkartScraper(query="laptop")
    flipkart_scraper1.single_threaded_scraper()
    flipkart_scraper2.multi_threaded_scraper(10)
    flipkart_scraper1.write_results_to_csv("earphones2.csv")
    flipkart_scraper2.write_results_to_csv("laptops2.csv")
