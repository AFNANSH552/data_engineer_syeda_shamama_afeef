import requests
import time
import logging
import random
import json
from abc import ABC, abstractmethod
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
from typing import Dict, List, Any, Optional
import pandas as pd
from tqdm import tqdm

from config import SCRAPING_CONFIG, DATA_CONFIG, LOG_FILE

class BaseScraper(ABC):
    """Base class for all scrapers with common functionality"""
    
    def __init__(self, category: str):
        self.category = category
        self.session = requests.Session()
        self.ua = UserAgent()
        self.setup_logging()
        self.scraped_data = []
        
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(LOG_FILE),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(f"{self.__class__.__name__}_{self.category}")
        
    def get_headers(self) -> Dict[str, str]:
        """Generate random headers to avoid detection"""
        return {
            'User-Agent': random.choice(SCRAPING_CONFIG['user_agents']),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        
    def make_request(self, url: str, max_retries: int = None) -> Optional[requests.Response]:
        """Make HTTP request with retry logic and error handling"""
        max_retries = max_retries or SCRAPING_CONFIG['retry_attempts']
        
        for attempt in range(max_retries):
            try:
                headers = self.get_headers()
                response = self.session.get(
                    url, 
                    headers=headers, 
                    timeout=SCRAPING_CONFIG['timeout']
                )
                response.raise_for_status()
                
                # Random delay to avoid rate limiting
                time.sleep(random.uniform(1, SCRAPING_CONFIG['request_delay']))
                
                self.logger.info(f"Successfully fetched: {url}")
                return response
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(random.uniform(3, 8))  # Longer delay between retries
                else:
                    self.logger.error(f"Failed to fetch {url} after {max_retries} attempts")
                    return None
                    
    def parse_response(self, response: requests.Response) -> BeautifulSoup:
        """Parse HTML response using BeautifulSoup"""
        return BeautifulSoup(response.content, 'html.parser')
        
    @abstractmethod
    def extract_product_data(self, soup: BeautifulSoup, url: str) -> List[Dict[str, Any]]:
        """Extract product data from parsed HTML - to be implemented by subclasses"""
        pass
        
    @abstractmethod
    def get_product_urls(self, soup: BeautifulSoup) -> List[str]:
        """Extract individual product URLs - to be implemented by subclasses"""
        pass
        
    def save_data(self, filename: str = None, format: str = 'json') -> str:
        """Save scraped data to file"""
        if not filename:
            filename = f"{self.category}_{int(time.time())}"
            
        if format.lower() == 'json':
            filepath = f"{DATA_CONFIG['raw_data_dir']}{filename}.json"
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.scraped_data, f, indent=2, ensure_ascii=False)
        elif format.lower() == 'csv':
            filepath = f"{DATA_CONFIG['raw_data_dir']}{filename}.csv"
            df = pd.DataFrame(self.scraped_data)
            df.to_csv(filepath, index=False, encoding='utf-8')
        else:
            raise ValueError("Format must be 'json' or 'csv'")
            
        self.logger.info(f"Data saved to: {filepath}")
        return filepath
        
    def scrape_category(self, urls: List[str], max_products: int = None) -> List[Dict[str, Any]]:
        """Main scraping method for a category"""
        max_products = max_products or DATA_CONFIG['max_products_per_category']
        
        self.logger.info(f"Starting scrape for category: {self.category}")
        self.logger.info(f"Target URLs: {urls}")
        
        for url in tqdm(urls, desc=f"Processing {self.category}"):
            if len(self.scraped_data) >= max_products:
                break
                
            response = self.make_request(url)
            if not response:
                continue
                
            soup = self.parse_response(response)
            
            try:
                # Extract data from current page
                page_data = self.extract_product_data(soup, url)
                self.scraped_data.extend(page_data)
                
                self.logger.info(f"Extracted {len(page_data)} products from {url}")
                
            except Exception as e:
                self.logger.error(f"Error extracting data from {url}: {str(e)}")
                continue
                
        self.logger.info(f"Scraping completed. Total products: {len(self.scraped_data)}")
        return self.scraped_data