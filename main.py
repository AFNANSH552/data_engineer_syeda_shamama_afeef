#!/usr/bin/env python3
"""
B2B Marketplace Data Scraper
Main script to orchestrate the scraping process
"""

import json
import logging
import argparse
from datetime import datetime

from config import PRODUCT_CATEGORIES, DATA_CONFIG
from src.scraper.indiamart_scraper import IndiaMartScraper
from src.data_processing.cleaner import DataCleaner

def setup_logging():
    """Setup logging for main script"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def scrape_category(category_name: str, category_config: dict, max_products: int = None) -> str:
    """Scrape a single product category"""
    logger = logging.getLogger(__name__)
    logger.info(f"Starting scrape for category: {category_name}")
    
    # Initialize scraper
    scraper = IndiaMartScraper(category_name)
    
    # Get URLs to scrape
    urls_to_scrape = category_config.get('indiamart_urls', [])
    
    # Also add search-based URLs
    for keyword in category_config.get('keywords', []):
        search_urls = scraper.search_products(keyword, max_pages=3)
        urls_to_scrape.extend(search_urls)
    
    # Remove duplicates
    urls_to_scrape = list(set(urls_to_scrape))
    logger.info(f"Total URLs to scrape: {len(urls_to_scrape)}")
    
    # Scrape data
    scraped_data = scraper.scrape_category(urls_to_scrape, max_products)
    
    if not scraped_data:
        logger.warning(f"No data scraped for category: {category_name}")
        return None
    
    # Save raw data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{category_name}_raw_{timestamp}"
    filepath = scraper.save_data(filename, format='json')
    
    logger.info(f"Scraped {len(scraped_data)} products for {category_name}")
    return filepath

def clean_and_process_data(raw_data_files: list) -> str:
    """Clean and process all scraped data"""
    logger = logging.getLogger(__name__)
    logger.info("Starting data cleaning and processing")
    
    # Initialize cleaner
    cleaner = DataCleaner()
    
    # Combine all raw data
    all_data = []
    for filepath in raw_data_files:
        if filepath:
            try:
                df = cleaner.load_data(filepath)
                all_data.append(df)
                logger.info(f"Loaded {len(df)} records from {filepath}")
            except Exception as e:
                logger.error(f"Error loading {filepath}: {str(e)}")
                continue
    
    if not all_data:
        logger.error("No data to process!")
        return None
    
    # Combine all dataframes
    import pandas as pd
    combined_df = pd.concat(all_data, ignore_index=True)
    logger.info(f"Combined dataset: {len(combined_df)} total records")
    
    # Clean the data
    cleaned_df, quality_report = cleaner.clean_dataset(combined_df)
    
    # Save cleaned data
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    cleaned_filepath = cleaner.save_cleaned_data(cleaned_df, f"cleaned_b2b_data_{timestamp}")
    
    # Save quality report
    report_filepath = f"{DATA_CONFIG['output_dir']}data_quality_report_{timestamp}.json"
    with open(report_filepath, 'w') as f:
        json.dump(quality_report, f, indent=2, default=str)
    
    logger.info(f"Data quality report saved to: {report_filepath}")
    logger.info(f"Final cleaned dataset: {len(cleaned_df)} records")
    
    return cleaned_filepath

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='B2B Marketplace Data Scraper')
    parser.add_argument('--categories', nargs='+', 
                       choices=list(PRODUCT_CATEGORIES.keys()),
                       default=list(PRODUCT_CATEGORIES.keys()),
                       help='Categories to scrape')
    parser.add_argument('--max-products', type=int, default=500,
                       help='Maximum products per category')
    parser.add_argument('--skip-scraping', action='store_true',
                       help='Skip scraping, only process existing data')
    
    args = parser.parse_args()
    
    logger = setup_logging()
    logger.info("Starting B2B Marketplace Data Scraper")
    logger.info(f"Target categories: {args.categories}")
    logger.info(f"Max products per category: {args.max_products}")
    
    raw_data_files = []
    
    if not args.skip_scraping:
        # Scrape each category
        for category in args.categories:
            if category in PRODUCT_CATEGORIES:
                try:
                    filepath = scrape_category(
                        category, 
                        PRODUCT_CATEGORIES[category], 
                        args.max_products
                    )
                    if filepath:
                        raw_data_files.append(filepath)
                except Exception as e:
                    logger.error(f"Error scraping {category}: {str(e)}")
                    continue
            else:
                logger.warning(f"Unknown category: {category}")
    
    # Process and clean data
    try:
        cleaned_filepath = clean_and_process_data(raw_data_files)
        if cleaned_filepath:
            logger.info(f"✅ Scraping completed successfully!")
            logger.info(f"📁 Cleaned data available at: {cleaned_filepath}")
            logger.info(f"🔍 Ready for EDA analysis!")
        else:
            logger.error("❌ Data processing failed!")
    except Exception as e:
        logger.error(f"Error in data processing: {str(e)}")

if __name__ == "__main__":
    main()