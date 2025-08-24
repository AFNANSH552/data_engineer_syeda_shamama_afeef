import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import warnings
from typing import Dict, List, Any
import logging
from wordcloud import WordCloud
import re
from collections import Counter

warnings.filterwarnings('ignore')

class B2BDataAnalyzer:
    """Comprehensive EDA for B2B marketplace data"""
    
    def __init__(self, data_path: str = None, df: pd.DataFrame = None):
        self.logger = logging.getLogger(__name__)
        
        if df is not None:
            self.df = df
        elif data_path:
            self.df = pd.read_csv(data_path)
        else:
            raise ValueError("Either data_path or df must be provided")
            
        self.setup_plotting_style()
        self.insights = {}
        
    def setup_plotting_style(self):
        plt.style.use('seaborn-v0_8')
        sns.set_palette("husl")
        
        self.plot_config = {
            'figure_size': (12, 8),
            'title_size': 16,
            'label_size': 12,
            'tick_size': 10
        }
        
    def basic_statistics(self) -> Dict[str, Any]:
        stats = {
            'dataset_shape': self.df.shape,
            'total_records': len(self.df),
            'total_columns': len(self.df.columns),
            'columns': list(self.df.columns),
            'data_types': self.df.dtypes.to_dict(),
            'missing_values': self.df.isnull().sum().to_dict(),
            'missing_percentages': (self.df.isnull().sum() * 100 / len(self.df)).round(2).to_dict()
        }
        stats['memory_usage'] = f"{self.df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB"
        self.insights['basic_stats'] = stats
        return stats
        
    def category_analysis(self) -> Dict[str, Any]:
        if 'category' not in self.df.columns:
            return {}
            
        category_stats = {
            'total_categories': self.df['category'].nunique(),
            'category_distribution': self.df['category'].value_counts().to_dict(),
            'category_percentages': (self.df['category'].value_counts(normalize=True) * 100).round(2).to_dict()
        }
        self.insights['categories'] = category_stats
        return category_stats
        
    def price_analysis(self) -> Dict[str, Any]:
        if 'numeric_price' not in self.df.columns:
            return {}
            
        price_data = self.df['numeric_price'].dropna()
        if len(price_data) == 0:
            return {'error': 'No valid price data found'}
            
        price_stats = {
            'total_products_with_price': len(price_data),
            'price_statistics': {
                'mean': float(price_data.mean()),
                'median': float(price_data.median()),
                'std': float(price_data.std()),
                'min': float(price_data.min()),
                'max': float(price_data.max()),
                'q25': float(price_data.quantile(0.25)),
                'q75': float(price_data.quantile(0.75))
            }
        }
        
        price_bins = [0, 100, 500, 1000, 5000, 10000, 50000, float('inf')]
        price_labels = ['<₹100', '₹100-500', '₹500-1K', '₹1K-5K', '₹5K-10K', '₹10K-50K', '>₹50K']
        price_data_binned = pd.cut(price_data, bins=price_bins, labels=price_labels, right=False)
        price_stats['price_range_distribution'] = price_data_binned.value_counts().to_dict()
        
        if 'category' in self.df.columns:
            price_by_category = self.df.groupby('category')['numeric_price'].agg(['count', 'mean', 'median']).round(2)
            price_stats['price_by_category'] = price_by_category.to_dict()
            
        self.insights['pricing'] = price_stats
        return price_stats

    def supplier_analysis(self) -> Dict[str, Any]:
        """Analyze supplier patterns"""
        if 'supplier_name' not in self.df.columns:
            return {}

        total_suppliers = self.df['supplier_name'].nunique()
        supplier_stats = {
            'total_suppliers': total_suppliers,
            'avg_products_per_supplier': round(len(self.df) / total_suppliers, 2) if total_suppliers > 0 else 0,
            'top_suppliers': self.df['supplier_name'].value_counts().head(10).to_dict()
        }

        supplier_counts = self.df['supplier_name'].value_counts()
        supplier_stats['supplier_concentration'] = {
            'single_product_suppliers': (supplier_counts == 1).sum(),
            'multi_product_suppliers': (supplier_counts > 1).sum(),
            'top_10_supplier_share': round((supplier_counts.head(10).sum() / len(self.df)) * 100, 2)
        }

        self.insights['suppliers'] = supplier_stats
        return supplier_stats

    def location_analysis(self) -> Dict[str, Any]:
        location_stats = {}
        
        if 'location' in self.df.columns:
            location_stats['raw_locations'] = {
                'total_locations': self.df['location'].nunique(),
                'top_locations': self.df['location'].value_counts().head(15).to_dict()
            }
            
        if 'extracted_state' in self.df.columns:
            state_data = self.df['extracted_state'].dropna()
            location_stats['state_analysis'] = {
                'total_states': state_data.nunique(),
                'state_distribution': state_data.value_counts().to_dict(),
                'state_percentages': (state_data.value_counts(normalize=True) * 100).round(2).to_dict()
            }
            
            if 'numeric_price' in self.df.columns:
                price_by_state = self.df.groupby('extracted_state')['numeric_price'].agg(['count', 'mean', 'median']).round(2)
                location_stats['price_by_state'] = price_by_state.to_dict()
                
        self.insights['location'] = location_stats
        return location_stats

    def text_analysis(self) -> Dict[str, Any]:
        text_stats = {}
        
        if 'title' in self.df.columns:
            titles = self.df['title'].dropna().astype(str)
            text_stats['title_analysis'] = {
                'avg_title_length': round(titles.str.len().mean(), 2),
                'title_word_count': titles.str.split().str.len().mean(),
                'common_title_words': self._get_common_words(titles, n=20)
            }
            
        if 'description' in self.df.columns:
            descriptions = self.df['description'].dropna().astype(str)
            if len(descriptions) > 0:
                text_stats['description_analysis'] = {
                    'products_with_description': len(descriptions),
                    'avg_description_length': round(descriptions.str.len().mean(), 2),
                    'common_description_words': self._get_common_words(descriptions, n=20)
                }
                
        self.insights['text_analysis'] = text_stats
        return text_stats

    def _get_common_words(self, text_series: pd.Series, n: int = 20) -> List[tuple]:
        all_text = ' '.join(text_series.values).lower()
        words = re.findall(r'\b[a-z]{3,}\b', all_text)
        stop_words = {'the', 'and', 'for', 'are', 'with', 'this', 'that', 'from', 'they', 'have', 
                      'was', 'been', 'said', 'each', 'which', 'she', 'you', 'one', 'our', 'had', 
                      'but', 'were', 'all', 'any', 'can', 'her', 'may', 'now', 'more', 'way'}
        filtered_words = [word for word in words if word not in stop_words]
        return Counter(filtered_words).most_common(n)

    def data_quality_assessment(self) -> Dict[str, Any]:
        quality_issues = {}
        
        missing_data = self.df.isnull().sum()
        quality_issues['missing_data'] = {
            'columns_with_missing': (missing_data > 0).sum(),
            'total_missing_values': missing_data.sum(),
            'missing_by_column': missing_data.to_dict()
        }
        
        if len(self.df) > 1:
            duplicates = self.df.duplicated().sum()
            quality_issues['duplicates'] = {
                'total_duplicates': duplicates,
                'duplicate_percentage': round((duplicates / len(self.df)) * 100, 2)
            }
            
        if 'numeric_price' in self.df.columns:
            price_data = self.df['numeric_price'].dropna()
            if len(price_data) > 0:
                q1 = price_data.quantile(0.25)
                q3 = price_data.quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                outliers = ((price_data < lower_bound) | (price_data > upper_bound)).sum()
                quality_issues['price_quality'] = {
                    'zero_prices': (price_data == 0).sum(),
                    'negative_prices': (price_data < 0).sum(),
                    'outlier_prices': outliers,
                    'outlier_percentage': round((outliers / len(price_data)) * 100, 2)
                }
                
        self.insights['data_quality'] = quality_issues
        return quality_issues

    def generate_comprehensive_report(self) -> Dict[str, Any]:
        self.logger.info("Generating comprehensive EDA report...")
        report = {
            'analysis_timestamp': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S'),
            'dataset_overview': self.basic_statistics(),
            'category_insights': self.category_analysis(),
            'pricing_insights': self.price_analysis(),
            'supplier_insights': self.supplier_analysis(),
            'location_insights': self.location_analysis(),
            'text_insights': self.text_analysis(),
            'data_quality': self.data_quality_assessment()
        }
        return report
