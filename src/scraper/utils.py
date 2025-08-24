"""
Utility functions for web scraping and data processing
"""

import re
import time
import random
import urllib.parse
from typing import List, Dict, Any, Optional
import pandas as pd
import logging

def clean_price_text(price_text: str) -> Dict[str, Any]:
    """
    Extract and clean price information from text
    
    Args:
        price_text: Raw price text from webpage
        
    Returns:
        Dictionary with cleaned price components
    """
    if not price_text:
        return {
            "raw_price": "",
            "numeric_price": None,
            "currency": "INR",
            "unit": "",
            "is_range": False
        }
    
    price_text = price_text.strip()
    
    # Handle price ranges (e.g., "₹100 - ₹500")
    range_match = re.search(r'₹?\s*(\d+(?:,\d+)*(?:\.\d+)?)\s*[-to]\s*₹?\s*(\d+(?:,\d+)*(?:\.\d+)?)', price_text, re.I)
    if range_match:
        min_price = float(range_match.group(1).replace(',', ''))
        max_price = float(range_match.group(2).replace(',', ''))
        avg_price = (min_price + max_price) / 2
        
        return {
            "raw_price": price_text,
            "numeric_price": avg_price,
            "min_price": min_price,
            "max_price": max_price,
            "currency": "INR",
            "unit": extract_price_unit(price_text),
            "is_range": True
        }
    
    # Single price extraction
    price_match = re.search(r'₹?\s*(\d+(?:,\d+)*(?:\.\d+)?)', price_text)
    numeric_price = None
    if price_match:
        numeric_price = float(price_match.group(1).replace(',', ''))
    
    return {
        "raw_price": price_text,
        "numeric_price": numeric_price,
        "currency": "INR",
        "unit": extract_price_unit(price_text),
        "is_range": False
    }

def extract_price_unit(price_text: str) -> str:
    """Extract unit from price text (per piece, per kg, etc.)"""
    if not price_text:
        return ""
    
    # Common unit patterns
    unit_patterns = [
        r'per\s+(\w+)',
        r'/(\w+)',
        r'(\w+)\s*$'
    ]
    
    price_lower = price_text.lower()
    
    for pattern in unit_patterns:
        match = re.search(pattern, price_lower)
        if match:
            unit = match.group(1)
            # Standardize common units
            unit_mapping = {
                'pc': 'piece', 'pcs': 'piece', 'piece': 'piece',
                'kg': 'kilogram', 'kilogram': 'kilogram',
                'gram': 'gram', 'gm': 'gram',
                'ton': 'ton', 'tonne': 'ton',
                'meter': 'meter', 'metre': 'meter', 'm': 'meter',
                'feet': 'feet', 'foot': 'feet', 'ft': 'feet',
                'litre': 'liter', 'liter': 'liter', 'l': 'liter',
                'dozen': 'dozen', 'pair': 'pair', 'set': 'set'
            }
            return unit_mapping.get(unit, unit)
    
    return ""

def normalize_location(location_text: str) -> Dict[str, str]:
    """
    Normalize and extract location information
    
    Args:
        location_text: Raw location text
        
    Returns:
        Dictionary with normalized location components
    """
    if not location_text:
        return {"raw_location": "", "city": "", "state": "", "normalized": ""}
    
    location_text = location_text.strip()
    
    # Indian states mapping (including common abbreviations)
    indian_states = {
        'andhra pradesh': 'Andhra Pradesh', 'ap': 'Andhra Pradesh',
        'arunachal pradesh': 'Arunachal Pradesh',
        'assam': 'Assam',
        'bihar': 'Bihar',
        'chhattisgarh': 'Chhattisgarh',
        'goa': 'Goa',
        'gujarat': 'Gujarat',
        'haryana': 'Haryana',
        'himachal pradesh': 'Himachal Pradesh', 'hp': 'Himachal Pradesh',
        'jharkhand': 'Jharkhand',
        'karnataka': 'Karnataka',
        'kerala': 'Kerala',
        'madhya pradesh': 'Madhya Pradesh', 'mp': 'Madhya Pradesh',
        'maharashtra': 'Maharashtra',
        'manipur': 'Manipur',
        'meghalaya': 'Meghalaya',
        'mizoram': 'Mizoram',
        'nagaland': 'Nagaland',
        'odisha': 'Odisha', 'orissa': 'Odisha',
        'punjab': 'Punjab',
        'rajasthan': 'Rajasthan',
        'sikkim': 'Sikkim',
        'tamil nadu': 'Tamil Nadu', 'tn': 'Tamil Nadu',
        'telangana': 'Telangana',
        'tripura': 'Tripura',
        'uttar pradesh': 'Uttar Pradesh', 'up': 'Uttar Pradesh',
        'uttarakhand': 'Uttarakhand',
        'west bengal': 'West Bengal', 'wb': 'West Bengal',
        'delhi': 'Delhi', 'new delhi': 'Delhi',
        'mumbai': 'Maharashtra', 'bangalore': 'Karnataka',
        'chennai': 'Tamil Nadu', 'kolkata': 'West Bengal',
        'hyderabad': 'Telangana', 'pune': 'Maharashtra',
        'ahmedabad': 'Gujarat', 'surat': 'Gujarat',
        'jaipur': 'Rajasthan', 'lucknow': 'Uttar Pradesh',
        'kanpur': 'Uttar Pradesh', 'nagpur': 'Maharashtra'
    }
    
    location_lower = location_text.lower()
    
    # Extract state
    extracted_state = ""
    for key, state in indian_states.items():
        if key in location_lower:
            extracted_state = state
            break
    
    # Extract city (first part before comma)
    city_match = re.match(r'^([^,]+)', location_text)
    city = city_match.group(1).strip() if city_match else ""
    
    return {
        "raw_location": location_text,
        "city": city,
        "state": extracted_state,
        "normalized": f"{city}, {extracted_state}" if city and extracted_state else location_text
    }

def validate_url(url: str, base_domain: str = None) -> bool:
    """
    Validate URL format and domain
    
    Args:
        url: URL to validate
        base_domain: Expected domain (optional)
        
    Returns:
        True if URL is valid
    """
    if not url:
        return False
    
    # Check basic URL format
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+), re.IGNORECASE)
    
    if not url_pattern.match(url):
        return False
    
    # Check domain if specified
    if base_domain:
        parsed_url = urllib.parse.urlparse(url)
        return base_domain.lower() in parsed_url.netloc.lower()
    
    return True

def smart_sleep(min_seconds: float = 1.0, max_seconds: float = 3.0):
    """
    Intelligent sleep with random delay to mimic human behavior
    
    Args:
        min_seconds: Minimum sleep time
        max_seconds: Maximum sleep time
    """
    sleep_time = random.uniform(min_seconds, max_seconds)
    time.sleep(sleep_time)

def extract_keywords_from_title(title: str, min_length: int = 3) -> List[str]:
    """
    Extract meaningful keywords from product titles
    
    Args:
        title: Product title
        min_length: Minimum word length
        
    Returns:
        List of keywords
    """
    if not title:
        return []
    
    # Remove special characters and normalize
    clean_title = re.sub(r'[^\w\s]', ' ', title.lower())
    words = clean_title.split()
    
    # Filter out common stop words and short words
    stop_words = {
        'and', 'or', 'the', 'a', 'an', 'in', 'on', 'at', 'to', 'for', 
        'of', 'with', 'by', 'from', 'as', 'is', 'are', 'was', 'were',
        'be', 'been', 'have', 'has', 'had', 'do', 'does', 'did', 'will',
        'would', 'could', 'should', 'can', 'may', 'might', 'must', 'shall'
    }
    
    keywords = [
        word for word in words 
        if len(word) >= min_length and word not in stop_words
    ]
    
    return keywords

def calculate_text_similarity(text1: str, text2: str) -> float:
    """
    Calculate similarity between two text strings using Jaccard similarity
    
    Args:
        text1: First text string
        text2: Second text string
        
    Returns:
        Similarity score between 0 and 1
    """
    if not text1 or not text2:
        return 0.0
    
    # Convert to sets of words
    words1 = set(text1.lower().split())
    words2 = set(text2.lower().split())
    
    # Calculate Jaccard similarity
    intersection = len(words1.intersection(words2))
    union = len(words1.union(words2))
    
    return intersection / union if union > 0 else 0.0

def detect_duplicate_products(df: pd.DataFrame, similarity_threshold: float = 0.7) -> pd.DataFrame:
    """
    Detect potential duplicate products using multiple criteria
    
    Args:
        df: DataFrame with product data
        similarity_threshold: Minimum similarity score for duplicates
        
    Returns:
        DataFrame with duplicate flags
    """
    logger = logging.getLogger(__name__)
    
    if len(df) < 2:
        df['is_duplicate'] = False
        return df
    
    df = df.copy()
    df['is_duplicate'] = False
    
    # Group by supplier to reduce comparison space
    for supplier in df['supplier_name'].unique():
        if pd.isna(supplier):
            continue
            
        supplier_products = df[df['supplier_name'] == supplier].copy()
        
        if len(supplier_products) < 2:
            continue
        
        # Compare products within the same supplier
        for i, row1 in supplier_products.iterrows():
            if df.loc[i, 'is_duplicate']:  # Already marked as duplicate
                continue
                
            for j, row2 in supplier_products.iterrows():
                if i >= j or df.loc[j, 'is_duplicate']:
                    continue
                
                # Calculate title similarity
                title_sim = calculate_text_similarity(
                    str(row1['title']), str(row2['title'])
                )
                
                # Check price similarity (within 10%)
                price_sim = 0.0
                if (pd.notna(row1['numeric_price']) and 
                    pd.notna(row2['numeric_price']) and 
                    row1['numeric_price'] > 0 and row2['numeric_price'] > 0):
                    
                    price_diff = abs(row1['numeric_price'] - row2['numeric_price'])
                    avg_price = (row1['numeric_price'] + row2['numeric_price']) / 2
                    price_sim = 1 - (price_diff / avg_price)
                    price_sim = max(0, price_sim)
                
                # Combined similarity score
                combined_sim = (title_sim * 0.8) + (price_sim * 0.2)
                
                if combined_sim >= similarity_threshold:
                    df.loc[j, 'is_duplicate'] = True
                    logger.info(f"Duplicate detected: {row1['title'][:50]}... vs {row2['title'][:50]}...")
    
    logger.info(f"Found {df['is_duplicate'].sum()} potential duplicates out of {len(df)} products")
    return df

def generate_product_id(title: str, supplier: str, price: float = None) -> str:
    """
    Generate a unique product identifier
    
    Args:
        title: Product title
        supplier: Supplier name  
        price: Product price (optional)
        
    Returns:
        Unique product identifier string
    """
    import hashlib
    
    # Normalize inputs
    title_norm = re.sub(r'[^\w\s]', '', str(title).lower()).strip()
    supplier_norm = re.sub(r'[^\w\s]', '', str(supplier).lower()).strip()
    
    # Create identifier string
    id_string = f"{title_norm}_{supplier_norm}"
    if price:
        id_string += f"_{int(price)}"
    
    # Generate hash
    product_id = hashlib.md5(id_string.encode()).hexdigest()[:12]
    
    return f"prod_{product_id}"

def validate_scraped_data(data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate scraped data quality
    
    Args:
        data: List of scraped product records
        
    Returns:
        Validation report dictionary
    """
    if not data:
        return {"valid": False, "errors": ["No data provided"]}
    
    errors = []
    warnings = []
    
    # Check required fields
    required_fields = ['title', 'supplier_name']
    for i, record in enumerate(data):
        for field in required_fields:
            if not record.get(field):
                errors.append(f"Record {i}: Missing required field '{field}'")
    
    # Check data quality
    titles_empty = sum(1 for record in data if not record.get('title', '').strip())
    prices_invalid = sum(1 for record in data 
                        if record.get('numeric_price') is not None and record['numeric_price'] <= 0)
    
    if titles_empty > len(data) * 0.1:  # More than 10% empty titles
        warnings.append(f"High number of empty titles: {titles_empty}")
    
    if prices_invalid > len(data) * 0.05:  # More than 5% invalid prices
        warnings.append(f"High number of invalid prices: {prices_invalid}")
    
    # Calculate quality score
    quality_score = max(0, 100 - (len(errors) * 10) - (len(warnings) * 2))
    
    return {
        "valid": len(errors) == 0,
        "total_records": len(data),
        "errors": errors,
        "warnings": warnings,
        "quality_score": quality_score,
        "summary": {
            "titles_empty": titles_empty,
            "prices_invalid": prices_invalid,
            "unique_suppliers": len(set(record.get('supplier_name', '') for record in data))
        }
    }