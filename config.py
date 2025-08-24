import os
from dotenv import load_dotenv

load_dotenv()

# Scraping Configuration
SCRAPING_CONFIG = {
    'user_agents': [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    ],
    'request_delay': 2,  # seconds between requests
    'retry_attempts': 3,
    'timeout': 30
}

# Target Categories
PRODUCT_CATEGORIES = {
    'industrial_machinery': {
        'keywords': ['industrial machinery', 'manufacturing equipment', 'heavy machinery'],
        'indiamart_urls': [
            'https://www.indiamart.com/industrial-machinery/',
            'https://www.indiamart.com/manufacturing-machines/'
        ]
    },
    'electronics': {
        'keywords': ['electronics', 'electronic components', 'electrical equipment'],
        'indiamart_urls': [
            'https://www.indiamart.com/electronics-electrical/',
            'https://www.indiamart.com/electronic-components/'
        ]
    },
    'textiles': {
        'keywords': ['textiles', 'fabric', 'yarn', 'clothing'],
        'indiamart_urls': [
            'https://www.indiamart.com/textiles/',
            'https://www.indiamart.com/fabric/'
        ]
    }
}

# Data Configuration
DATA_CONFIG = {
    'raw_data_dir': 'data/raw/',
    'processed_data_dir': 'data/processed/',
    'output_dir': 'data/outputs/',
    'log_dir': 'logs/',
    'max_products_per_category': 1000
}

# File paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
LOG_FILE = os.path.join(BASE_DIR, 'logs', 'scraper.log')

# Create directories if they don't exist
os.makedirs(DATA_CONFIG['raw_data_dir'], exist_ok=True)
os.makedirs(DATA_CONFIG['processed_data_dir'], exist_ok=True)
os.makedirs(DATA_CONFIG['output_dir'], exist_ok=True)
os.makedirs(DATA_CONFIG['log_dir'], exist_ok=True)