import re
import urllib.parse
from typing import Dict, List, Any, Optional
from bs4 import BeautifulSoup

from .base_scraper import BaseScraper

class IndiaMartScraper(BaseScraper):
    """IndiaMART specific scraper implementation"""
    
    def __init__(self, category: str):
        super().__init__(category)
        self.base_url = "https://www.indiamart.com"
        
    def clean_text(self, text: str) -> str:
        """Clean and normalize text data"""
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text.strip())
        
    def extract_price(self, price_text: str) -> Dict[str, Any]:
        """Extract and parse price information"""
        if not price_text:
            return {"raw_price": "", "numeric_price": None, "currency": "INR", "unit": ""}
            
        price_text = self.clean_text(price_text)
        
        # Extract numeric price
        price_match = re.search(r'₹?\s*(\d+(?:,\d+)*(?:\.\d+)?)', price_text)
        numeric_price = None
        if price_match:
            numeric_price = float(price_match.group(1).replace(',', ''))
            
        # Extract unit
        unit_match = re.search(r'per\s+(\w+)|/(\w+)', price_text.lower())
        unit = ""
        if unit_match:
            unit = unit_match.group(1) or unit_match.group(2)
            
        return {
            "raw_price": price_text,
            "numeric_price": numeric_price,
            "currency": "INR",
            "unit": unit
        }
        
    def extract_product_data(self, soup: BeautifulSoup, url: str) -> List[Dict[str, Any]]:
        """Extract product data from IndiaMART page"""
        products = []
        
        # Multiple selectors to handle different page layouts
        product_selectors = [
            '.prd',
            '.lst',
            '.product-item',
            '.srp-list-item',
            '.prd-item'
        ]
        
        product_elements = []
        for selector in product_selectors:
            elements = soup.select(selector)
            if elements:
                product_elements = elements
                break
                
        if not product_elements:
            # Fallback: try to find any div with product-related class names
            product_elements = soup.find_all('div', class_=re.compile(r'(product|prd|item)', re.I))
            
        for element in product_elements:
            try:
                product_data = self.extract_single_product(element, url)
                if product_data:
                    products.append(product_data)
            except Exception as e:
                self.logger.warning(f"Error extracting single product: {str(e)}")
                continue
                
        return products
        
    def extract_single_product(self, element: BeautifulSoup, source_url: str) -> Optional[Dict[str, Any]]:
        """Extract data from a single product element"""
        
        # Product name/title
        title_selectors = ['.prd-name', '.lst-name', '.product-name', 'h3', 'h4', '.title']
        title = ""
        for selector in title_selectors:
            title_elem = element.select_one(selector)
            if title_elem:
                title = self.clean_text(title_elem.get_text())
                break
                
        if not title:
            return None
            
        # Price
        price_selectors = ['.prd-price', '.lst-price', '.price', '.prd-prc']
        price_text = ""
        for selector in price_selectors:
            price_elem = element.select_one(selector)
            if price_elem:
                price_text = self.clean_text(price_elem.get_text())
                break
                
        price_info = self.extract_price(price_text)
        
        # Supplier/Company name
        supplier_selectors = ['.prd-comp', '.lst-comp', '.company-name', '.supplier']
        supplier = ""
        for selector in supplier_selectors:
            supplier_elem = element.select_one(selector)
            if supplier_elem:
                supplier = self.clean_text(supplier_elem.get_text())
                break
                
        # Location
        location_selectors = ['.prd-loc', '.lst-loc', '.location', '.city']
        location = ""
        for selector in location_selectors:
            location_elem = element.select_one(selector)
            if location_elem:
                location = self.clean_text(location_elem.get_text())
                break
                
        # Product image
        img_elem = element.find('img')
        image_url = ""
        if img_elem:
            image_url = img_elem.get('src') or img_elem.get('data-src') or ""
            if image_url and not image_url.startswith('http'):
                image_url = urllib.parse.urljoin(self.base_url, image_url)
                
        # Product link
        link_elem = element.find('a')
        product_url = ""
        if link_elem:
            product_url = link_elem.get('href', '')
            if product_url and not product_url.startswith('http'):
                product_url = urllib.parse.urljoin(self.base_url, product_url)
                
        # Additional attributes
        description = ""
        desc_selectors = ['.prd-desc', '.description', '.details']
        for selector in desc_selectors:
            desc_elem = element.select_one(selector)
            if desc_elem:
                description = self.clean_text(desc_elem.get_text())
                break
                
        return {
            "title": title,
            "supplier_name": supplier,
            "location": location,
            "raw_price": price_info["raw_price"],
            "numeric_price": price_info["numeric_price"],
            "currency": price_info["currency"],
            "price_unit": price_info["unit"],
            "description": description,
            "image_url": image_url,
            "product_url": product_url,
            "source_url": source_url,
            "category": self.category,
            "marketplace": "IndiaMART"
        }
        
    def get_product_urls(self, soup: BeautifulSoup) -> List[str]:
        """Extract individual product URLs from listing page"""
        urls = []
        
        # Find all product links
        link_selectors = [
            '.prd a',
            '.lst a',
            '.product-item a',
            'a[href*="/proddetail/"]'
        ]
        
        for selector in link_selectors:
            links = soup.select(selector)
            for link in links:
                href = link.get('href')
                if href:
                    if not href.startswith('http'):
                        href = urllib.parse.urljoin(self.base_url, href)
                    urls.append(href)
                    
        return list(set(urls))  # Remove duplicates
        
    def search_products(self, query: str, max_pages: int = 5) -> List[str]:
        """Search for products and return URLs"""
        search_urls = []
        
        # Encode search query
        encoded_query = urllib.parse.quote(query)
        
        for page in range(1, max_pages + 1):
            search_url = f"{self.base_url}/search.mp?ss={encoded_query}&page={page}"
            search_urls.append(search_url)
            
        return search_urls