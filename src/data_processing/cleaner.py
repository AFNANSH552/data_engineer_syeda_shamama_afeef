import pandas as pd
import numpy as np
import re
import logging
from typing import Dict, List, Any, Optional
from config import DATA_CONFIG

class DataCleaner:
    """Clean and standardize scraped data"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def load_data(self, filepath: str) -> pd.DataFrame:
        """Load data from JSON or CSV file"""
        try:
            if filepath.endswith('.json'):
                df = pd.read_json(filepath)
            elif filepath.endswith('.csv'):
                df = pd.read_csv(filepath)
            else:
                raise ValueError("File must be JSON or CSV format")
                
            self.logger.info(f"Loaded {len(df)} records from {filepath}")
            return df
            
        except Exception as e:
            self.logger.error(f"Error loading data: {str(e)}")
            raise
            
    def clean_text_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean text columns by removing extra whitespace and standardizing"""
        text_columns = ['title', 'supplier_name', 'location', 'description']
        
        for col in text_columns:
            if col in df.columns:
                df[col] = df[col].astype(str)
                df[col] = df[col].str.strip()
                df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
                df[col] = df[col].replace(['nan', 'None', ''], np.nan)
                
        return df
        
    def standardize_prices(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize price information"""
        if 'numeric_price' not in df.columns:
            return df
            
        # Convert to numeric, handling any string values
        df['numeric_price'] = pd.to_numeric(df['numeric_price'], errors='coerce')
        
        # Remove unrealistic prices (likely errors)
        price_q1 = df['numeric_price'].quantile(0.01)
        price_q99 = df['numeric_price'].quantile(0.99)
        
        # Flag outliers but don't remove them yet
        df['price_outlier'] = (
            (df['numeric_price'] < price_q1) | 
            (df['numeric_price'] > price_q99)
        )
        
        # Standardize price units
        if 'price_unit' in df.columns:
            unit_mapping = {
                'piece': 'piece',
                'pieces': 'piece',
                'pc': 'piece',
                'pcs': 'piece',
                'unit': 'piece',
                'kg': 'kilogram',
                'kilogram': 'kilogram',
                'gram': 'gram',
                'ton': 'ton',
                'tonne': 'ton',
                'meter': 'meter',
                'metre': 'meter',
                'm': 'meter',
                'feet': 'feet',
                'foot': 'feet',
                'inch': 'inch',
                'litre': 'liter',
                'liter': 'liter',
                'l': 'liter'
            }
            
            df['price_unit'] = df['price_unit'].str.lower()
            df['standardized_unit'] = df['price_unit'].map(unit_mapping).fillna(df['price_unit'])
            
        return df
        
    def standardize_locations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize location information"""
        if 'location' not in df.columns:
            return df
            
        # Extract state from location
        indian_states = {
            'maharashtra', 'uttar pradesh', 'up', 'karnataka', 'tamil nadu', 'tn',
            'gujarat', 'rajasthan', 'west bengal', 'wb', 'madhya pradesh', 'mp',
            'andhra pradesh', 'ap', 'odisha', 'orissa', 'telangana', 'kerala',
            'punjab', 'haryana', 'jharkhand', 'bihar', 'chhattisgarh', 'uttarakhand',
            'himachal pradesh', 'hp', 'assam', 'jammu and kashmir', 'j&k',
            'delhi', 'new delhi', 'mumbai', 'bangalore', 'chennai', 'kolkata',
            'hyderabad', 'pune', 'ahmedabad', 'surat', 'jaipur', 'lucknow',
            'kanpur', 'nagpur', 'visakhapatnam', 'indore', 'thane', 'bhopal'
        }
        
        def extract_state(location_text):
            if pd.isna(location_text):
                return None
                
            location_lower = str(location_text).lower()
            for state in indian_states:
                if state in location_lower:
                    return state.title()
            return None
            
        df['extracted_state'] = df['location'].apply(extract_state)
        
        return df
        
    def remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate products based on title and supplier"""
        initial_count = len(df)
        
        # Create a composite key for deduplication
        df['dedup_key'] = (
            df['title'].fillna('').str.lower() + '_' + 
            df['supplier_name'].fillna('').str.lower()
        )
        
        # Keep first occurrence of each unique product
        df_dedup = df.drop_duplicates(subset='dedup_key', keep='first')
        
        # Remove the temporary key
        df_dedup = df_dedup.drop('dedup_key', axis=1)
        
        removed_count = initial_count - len(df_dedup)
        self.logger.info(f"Removed {removed_count} duplicates. Remaining: {len(df_dedup)}")
        
        return df_dedup
        
    def validate_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate data quality report"""
        report = {
            'total_records': len(df),
            'columns': list(df.columns),
            'missing_values': df.isnull().sum().to_dict(),
            'data_types': df.dtypes.to_dict()
        }
        
        # Quality metrics
        if 'title' in df.columns:
            report['products_with_title'] = df['title'].notna().sum()
            report['avg_title_length'] = df['title'].str.len().mean()
            
        if 'numeric_price' in df.columns:
            price_data = df['numeric_price'].dropna()
            if len(price_data) > 0:
                report['price_statistics'] = {
                    'count': len(price_data),
                    'mean': float(price_data.mean()),
                    'median': float(price_data.median()),
                    'min': float(price_data.min()),
                    'max': float(price_data.max()),
                    'std': float(price_data.std())
                }
                
        if 'supplier_name' in df.columns:
            report['unique_suppliers'] = df['supplier_name'].nunique()
            
        if 'location' in df.columns:
            report['unique_locations'] = df['location'].nunique()
            
        if 'category' in df.columns:
            report['category_distribution'] = df['category'].value_counts().to_dict()
            
        return report
        
    def clean_dataset(self, df: pd.DataFrame) -> tuple[pd.DataFrame, Dict[str, Any]]:
        """Main cleaning pipeline"""
        self.logger.info(f"Starting data cleaning for {len(df)} records")
        
        # Apply cleaning steps
        df = self.clean_text_columns(df)
        df = self.standardize_prices(df)
        df = self.standardize_locations(df)
        df = self.remove_duplicates(df)
        
        # Generate quality report
        quality_report = self.validate_data_quality(df)
        
        self.logger.info(f"Cleaning completed. Final dataset: {len(df)} records")
        
        return df, quality_report
        
    def save_cleaned_data(self, df: pd.DataFrame, filename: str) -> str:
        """Save cleaned data to processed directory"""
        filepath = f"{DATA_CONFIG['processed_data_dir']}{filename}.csv"
        df.to_csv(filepath, index=False, encoding='utf-8')
        
        self.logger.info(f"Cleaned data saved to: {filepath}")
        return filepath