#!/usr/bin/env python3
"""
B2B Marketplace Data - Exploratory Data Analysis
Complete EDA pipeline with insights and visualizations
"""

import pandas as pd
import json
import argparse
import logging
from datetime import datetime
from pathlib import Path

from src.analysis.eda import B2BDataAnalyzer
from src.analysis.visualizations import B2BDataVisualizer
from config import DATA_CONFIG

def setup_logging():
    """Setup logging for EDA script"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(__name__)

def find_latest_data_file(directory: str) -> str:
    """Find the most recent cleaned data file"""
    data_dir = Path(directory)
    
    # Look for CSV files
    csv_files = list(data_dir.glob("cleaned_*.csv"))
    
    if not csv_files:
        raise FileNotFoundError(f"No cleaned data files found in {directory}")
    
    # Return the most recent file
    latest_file = max(csv_files, key=lambda x: x.stat().st_mtime)
    return str(latest_file)

def run_comprehensive_eda(data_path: str, output_dir: str = None) -> dict:
    """Run complete EDA analysis"""
    
    logger = logging.getLogger(__name__)
    
    # Setup output directory
    if not output_dir:
        output_dir = DATA_CONFIG['output_dir']
    
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Starting comprehensive EDA analysis...")
    logger.info(f"Data source: {data_path}")
    logger.info(f"Output directory: {output_dir}")
    
    # Load data
    logger.info("Loading data...")
    df = pd.read_csv(data_path)
    logger.info(f"Loaded dataset: {df.shape[0]} rows, {df.shape[1]} columns")
    
    # Initialize analyzer
    analyzer = B2BDataAnalyzer(df=df)
    
    # Generate comprehensive insights
    logger.info("Generating comprehensive insights...")
    insights = analyzer.generate_comprehensive_report()
    
    # Print key insights to console
    print_key_insights(insights)
    
    # Save insights report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    insights_file = f"{output_dir}eda_insights_{timestamp}.json"
    
    with open(insights_file, 'w') as f:
        json.dump(insights, f, indent=2, default=str)
    
    logger.info(f"Insights saved to: {insights_file}")
    
    # Create visualizations
    logger.info("Creating visualizations...")
    visualizer = B2BDataVisualizer(df, output_dir)
    dashboard = visualizer.create_comprehensive_dashboard(save=True)
    
    # Create executive summary
    logger.info("Creating executive summary...")
    summary_plot = visualizer.create_summary_report_plot(insights, save=True)
    
    logger.info("EDA analysis completed successfully!")
    
    return {
        'insights': insights,
        'dashboard': dashboard,
        'summary_plot': summary_plot,
        'data_shape': df.shape,
        'output_directory': output_dir
    }

def print_key_insights(insights: dict):
    """Print key insights to console"""
    
    print("\n" + "="*60)
    print("🔍 KEY INSIGHTS - B2B MARKETPLACE DATA ANALYSIS")
    print("="*60)
    
    # Dataset Overview
    if 'dataset_overview' in insights:
        stats = insights['dataset_overview']
        print(f"\n📊 DATASET OVERVIEW:")
        print(f"   • Total Records: {stats['total_records']:,}")
        print(f"   • Total Columns: {stats['total_columns']}")
        print(f"   • Memory Usage: {stats['memory_usage']}")
        
        # Missing data summary
        missing = stats['missing_percentages']
        high_missing = {k: v for k, v in missing.items() if v > 10}
        if high_missing:
            print(f"   • Columns with >10% missing data: {len(high_missing)}")
    
    # Category Insights
    if 'category_insights' in insights:
        cat_data = insights['category_insights']
        print(f"\n📦 PRODUCT CATEGORIES:")
        print(f"   • Total Categories: {cat_data['total_categories']}")
        
        top_categories = list(cat_data['category_distribution'].items())[:3]
        print(f"   • Top Categories:")
        for cat, count in top_categories:
            pct = cat_data['category_percentages'][cat]
            print(f"     - {cat}: {count:,} products ({pct}%)")
    
    # Pricing Insights
    if 'pricing_insights' in insights and 'price_statistics' in insights['pricing_insights']:
        price_stats = insights['pricing_insights']['price_statistics']
        print(f"\n💰 PRICING ANALYSIS:")
        print(f"   • Products with Price: {insights['pricing_insights']['total_products_with_price']:,}")
        print(f"   • Average Price: ₹{price_stats['mean']:,.2f}")
        print(f"   • Median Price: ₹{price_stats['median']:,.2f}")
        print(f"   • Price Range: ₹{price_stats['min']:,.2f} - ₹{price_stats['max']:,.2f}")
        
        # Price ranges
        if 'price_range_distribution' in insights['pricing_insights']:
            price_ranges = insights['pricing_insights']['price_range_distribution']
            print(f"   • Most Common Price Range: {max(price_ranges, key=price_ranges.get)}")
    
    # Supplier Insights
    if 'supplier_insights' in insights:
        supp_data = insights['supplier_insights']
        print(f"\n🏢 SUPPLIER ANALYSIS:")
        print(f"   • Total Suppliers: {supp_data['total_suppliers']:,}")
        print(f"   • Avg Products/Supplier: {supp_data['avg_products_per_supplier']}")
        
        conc_data = supp_data['supplier_concentration']
        print(f"   • Single Product Suppliers: {conc_data['single_product_suppliers']:,}")
        print(f"   • Multi Product Suppliers: {conc_data['multi_product_suppliers']:,}")
        print(f"   • Top 10 Supplier Market Share: {conc_data['top_10_supplier_share']}%")
    
    # Location Insights
    if 'location_insights' in insights:
        loc_data = insights['location_insights']
        
        if 'state_analysis' in loc_data:
            state_data = loc_data['state_analysis']
            print(f"\n🌍 GEOGRAPHICAL DISTRIBUTION:")
            print(f"   • States Covered: {state_data['total_states']}")
            
            top_states = list(state_data['state_distribution'].items())[:3]
            print(f"   • Top States by Suppliers:")
            for state, count in top_states:
                pct = state_data['state_percentages'][state]
                print(f"     - {state}: {count:,} suppliers ({pct}%)")
    
    # Data Quality
    if 'data_quality' in insights:
        quality = insights['data_quality']
        
        print(f"\n⚠️  DATA QUALITY ASSESSMENT:")
        if 'missing_data' in quality:
            missing = quality['missing_data']
            print(f"   • Columns with Missing Data: {missing['columns_with_missing']}")
            print(f"   • Total Missing Values: {missing['total_missing_values']:,}")
        
        if 'duplicates' in quality:
            dup = quality['duplicates']
            print(f"   • Duplicate Records: {dup['total_duplicates']:,} ({dup['duplicate_percentage']}%)")
        
        if 'price_quality' in quality:
            price_q = quality['price_quality']
            print(f"   • Price Quality Issues:")
            print(f"     - Zero Prices: {price_q['zero_prices']:,}")
            print(f"     - Price Outliers: {price_q['outlier_prices']:,} ({price_q['outlier_percentage']}%)")
    
    # Text Analysis
    if 'text_insights' in insights:
        text_data = insights['text_insights']
        
        if 'title_analysis' in text_data:
            title_data = text_data['title_analysis']
            print(f"\n📝 TEXT ANALYSIS:")
            print(f"   • Avg Title Length: {title_data['avg_title_length']:.1f} characters")
            print(f"   • Avg Words per Title: {title_data['title_word_count']:.1f}")
            
            if 'common_title_words' in title_data:
                top_words = title_data['common_title_words'][:5]
                print(f"   • Most Common Words: {', '.join([word[0] for word in top_words])}")
    
    print("\n" + "="*60)
    print("✅ Analysis complete! Check output directory for detailed visualizations.")
    print("="*60 + "\n")

def generate_insights_summary(insights: dict, output_path: str):
    """Generate a human-readable summary report"""
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    summary_text = f"""
# B2B Marketplace Data Analysis Report
Generated on: {timestamp}

## Executive Summary

This report presents key findings from the analysis of B2B marketplace data scraped from IndiaMART.
The dataset contains product listings across multiple categories with supplier and pricing information.

"""
    
    # Add detailed sections based on available insights
    if 'dataset_overview' in insights:
        stats = insights['dataset_overview']
        summary_text += f"""
## Dataset Overview

- **Total Records**: {stats['total_records']:,}
- **Total Columns**: {stats['total_columns']}
- **Data Size**: {stats['memory_usage']}
- **Analysis Date**: {insights['analysis_timestamp']}

"""
    
    # Add more sections based on available insights...
    # (This can be expanded based on requirements)
    
    # Save summary
    with open(output_path, 'w') as f:
        f.write(summary_text)

def main():
    """Main execution function for EDA"""
    
    parser = argparse.ArgumentParser(description='B2B Marketplace Data - EDA Analysis')
    parser.add_argument('--data-path', type=str, help='Path to cleaned data CSV file')
    parser.add_argument('--output-dir', type=str, 
                       default=DATA_CONFIG['output_dir'],
                       help='Output directory for results')
    parser.add_argument('--auto-find', action='store_true',
                       help='Automatically find latest cleaned data file')
    
    args = parser.parse_args()
    
    logger = setup_logging()
    
    # Determine data path
    if args.auto_find:
        try:
            data_path = find_latest_data_file(DATA_CONFIG['processed_data_dir'])
            logger.info(f"Auto-found data file: {data_path}")
        except FileNotFoundError as e:
            logger.error(str(e))
            return
    elif args.data_path:
        data_path = args.data_path
    else:
        logger.error("Please provide --data-path or use --auto-find")
        return
    
    try:
        # Run comprehensive EDA
        results = run_comprehensive_eda(data_path, args.output_dir)
        
        # Generate summary report
        summary_path = f"{args.output_dir}analysis_summary.md"
        generate_insights_summary(results['insights'], summary_path)
        
        logger.info(f"📊 EDA Results:")
        logger.info(f"   • Dataset Shape: {results['data_shape']}")
        logger.info(f"   • Output Directory: {results['output_directory']}")
        logger.info(f"   • Summary Report: {summary_path}")
        logger.info(f"   • Interactive Dashboards: Check HTML files in output directory")
        
    except Exception as e:
        logger.error(f"EDA analysis failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()