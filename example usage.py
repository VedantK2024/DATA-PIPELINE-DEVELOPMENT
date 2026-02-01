"""
ETL Pipeline Example Usage
===========================

This script demonstrates how to use the ETL Pipeline with sample data.
It includes examples of different data sources, transformations, and outputs.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from etl_pipeline import ETLPipeline


def create_sample_data():
    """Create sample dataset for demonstration."""
    np.random.seed(42)
    
    # Generate sample data
    n_records = 1000
    
    data = {
        'customer_id': range(1, n_records + 1),
        'name': [f'Customer_{i}' for i in range(1, n_records + 1)],
        'age': np.random.randint(18, 80, n_records),
        'income': np.random.normal(50000, 20000, n_records),
        'category': np.random.choice(['A', 'B', 'C', 'D'], n_records),
        'purchase_date': [
            datetime.now() - timedelta(days=np.random.randint(0, 365))
            for _ in range(n_records)
        ],
        'purchase_amount': np.random.exponential(100, n_records),
        'region': np.random.choice(['North', 'South', 'East', 'West'], n_records)
    }
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Introduce some data quality issues
    # Missing values
    missing_indices = np.random.choice(df.index, size=50, replace=False)
    df.loc[missing_indices, 'age'] = np.nan
    df.loc[np.random.choice(df.index, size=30, replace=False), 'income'] = np.nan
    
    # Duplicates
    duplicates = df.sample(n=20)
    df = pd.concat([df, duplicates], ignore_index=True)
    
    # Outliers
    outlier_indices = np.random.choice(df.index, size=10, replace=False)
    df.loc[outlier_indices, 'income'] = df['income'].max() * 3
    
    return df


def example_1_basic_pipeline():
    """Example 1: Basic ETL pipeline with CSV input/output."""
    print("\n" + "=" * 60)
    print("EXAMPLE 1: Basic ETL Pipeline")
    print("=" * 60)
    
    # Create sample data
    df = create_sample_data()
    df.to_csv('sample_input.csv', index=False)
    print(f"Created sample data: {df.shape[0]} rows, {df.shape[1]} columns")
    
    # Initialize pipeline
    pipeline = ETLPipeline()
    
    # Configure pipeline
    source_config = {
        'type': 'csv',
        'filepath': 'sample_input.csv'
    }
    
    transform_config = [
        {
            'operation': 'handle_missing',
            'strategy': 'mean'
        },
        {
            'operation': 'remove_duplicates'
        },
        {
            'operation': 'handle_outliers',
            'columns': ['income', 'purchase_amount'],
            'method': 'iqr',
            'threshold': 1.5
        }
    ]
    
    load_config = {
        'type': 'csv',
        'filepath': 'cleaned_output.csv'
    }
    
    # Run pipeline
    metadata = pipeline.run_pipeline(source_config, transform_config, load_config)
    pipeline.save_metadata('example1_metadata.json')
    
    print(f"\nTransformations applied: {metadata['transformations_applied']}")
    print(f"Pipeline duration: {metadata['pipeline_duration']:.2f} seconds")


def example_2_advanced_transformations():
    """Example 2: Advanced transformations with encoding and feature engineering."""
    print("\n" + "=" * 60)
    print("EXAMPLE 2: Advanced Transformations")
    print("=" * 60)
    
    # Create sample data
    df = create_sample_data()
    df.to_csv('sample_input2.csv', index=False)
    
    # Initialize pipeline
    pipeline = ETLPipeline()
    
    # Configure pipeline with advanced transformations
    source_config = {
        'type': 'csv',
        'filepath': 'sample_input2.csv'
    }
    
    transform_config = [
        {
            'operation': 'handle_missing',
            'strategy': 'median',
            'columns': ['age', 'income']
        },
        {
            'operation': 'remove_duplicates'
        },
        {
            'operation': 'datetime_features',
            'date_column': 'purchase_date'
        },
        {
            'operation': 'encode_categorical',
            'columns': ['category', 'region'],
            'method': 'label'
        },
        {
            'operation': 'normalize',
            'columns': ['age', 'income', 'purchase_amount'],
            'method': 'minmax'
        }
    ]
    
    load_config = {
        'type': 'csv',
        'filepath': 'transformed_output.csv'
    }
    
    # Run pipeline
    metadata = pipeline.run_pipeline(source_config, transform_config, load_config)
    pipeline.save_metadata('example2_metadata.json')
    
    print(f"\nFinal data shape: {metadata['final_shape']}")
    print(f"Transformations: {metadata['transformations_applied']}")


def example_3_multiple_outputs():
    """Example 3: Load data to multiple output formats."""
    print("\n" + "=" * 60)
    print("EXAMPLE 3: Multiple Output Formats")
    print("=" * 60)
    
    # Create and clean data
    df = create_sample_data()
    df.to_csv('sample_input3.csv', index=False)
    
    # Initialize pipeline
    pipeline = ETLPipeline()
    
    # Extract and transform
    pipeline.data = pipeline.extract_csv('sample_input3.csv')
    pipeline.data = pipeline.handle_missing_values(pipeline.data, strategy='mean')
    pipeline.data = pipeline.remove_duplicates(pipeline.data)
    
    # Validate data
    validation = pipeline.validate_data(pipeline.data)
    print(f"\nData quality check:")
    print(f"  Total rows: {validation['total_rows']}")
    print(f"  Duplicate rows: {validation['duplicate_rows']}")
    print(f"  Issues found: {len(validation['issues'])}")
    
    # Generate data profile
    profile = pipeline.generate_data_profile(pipeline.data)
    print(f"\nData profile:")
    print(f"  Memory usage: {profile['memory_usage']:.2f} MB")
    print(f"  Number of columns: {len(profile['columns'])}")
    
    # Load to multiple formats
    pipeline.load_csv(pipeline.data, 'output.csv')
    pipeline.load_json(pipeline.data, 'output.json', orient='records')
    pipeline.load_excel(pipeline.data, 'output.xlsx', sheet_name='Cleaned_Data')
    
    try:
        pipeline.load_parquet(pipeline.data, 'output.parquet')
        print("\nData saved to: CSV, JSON, Excel, Parquet")
    except ImportError:
        print("\nData saved to: CSV, JSON, Excel (Parquet requires pyarrow)")


def example_4_custom_transformation():
    """Example 4: Using custom transformation functions."""
    print("\n" + "=" * 60)
    print("EXAMPLE 4: Custom Transformations")
    print("=" * 60)
    
    # Create sample data
    df = create_sample_data()
    
    # Initialize pipeline
    pipeline = ETLPipeline()
    pipeline.data = df
    
    # Define custom transformation
    def custom_feature_engineering(df):
        """Create custom features."""
        df = df.copy()
        
        # Create age groups
        df['age_group'] = pd.cut(df['age'], bins=[0, 30, 50, 100], 
                                 labels=['Young', 'Middle', 'Senior'])
        
        # Create income categories
        df['income_category'] = pd.qcut(df['income'], q=4, 
                                        labels=['Low', 'Medium', 'High', 'Very High'])
        
        # Calculate income-to-purchase ratio
        df['income_purchase_ratio'] = df['income'] / (df['purchase_amount'] + 1)
        
        return df
    
    # Apply custom transformation
    pipeline.data = pipeline.apply_custom_transformation(
        pipeline.data, 
        custom_feature_engineering
    )
    
    print(f"\nNew columns created: age_group, income_category, income_purchase_ratio")
    print(f"Final shape: {pipeline.data.shape}")
    
    # Save results
    pipeline.load_csv(pipeline.data, 'custom_features_output.csv')


def example_5_data_validation():
    """Example 5: Comprehensive data validation."""
    print("\n" + "=" * 60)
    print("EXAMPLE 5: Data Validation")
    print("=" * 60)
    
    # Create sample data with issues
    df = create_sample_data()
    
    # Initialize pipeline
    pipeline = ETLPipeline()
    
    # Define custom validation rules
    validation_rules = {
        'age_range': lambda df: all(df['age'].between(0, 120)),
        'positive_income': lambda df: all(df['income'] > 0),
        'valid_categories': lambda df: df['category'].isin(['A', 'B', 'C', 'D']).all()
    }
    
    # Validate
    results = pipeline.validate_data(df, rules=validation_rules)
    
    print("\nValidation Results:")
    print(f"  Total rows: {results['total_rows']}")
    print(f"  Duplicate rows: {results['duplicate_rows']}")
    print(f"\n  Missing values:")
    for col, count in results['missing_values'].items():
        if count > 0:
            print(f"    {col}: {count}")
    
    print(f"\n  Issues found: {len(results['issues'])}")
    for issue in results['issues']:
        print(f"    - {issue}")
    
    # Generate profile
    profile = pipeline.generate_data_profile(df)
    
    print(f"\n  Data Profile Summary:")
    print(f"    Shape: {profile['shape']}")
    print(f"    Memory: {profile['memory_usage']:.2f} MB")


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print(" " * 20 + "ETL PIPELINE EXAMPLES")
    print("=" * 80)
    
    # Run all examples
    try:
        example_1_basic_pipeline()
        example_2_advanced_transformations()
        example_3_multiple_outputs()
        example_4_custom_transformation()
        example_5_data_validation()
        
        print("\n" + "=" * 80)
        print(" " * 25 + "ALL EXAMPLES COMPLETED")
        print("=" * 80)
        
    except Exception as e:
        print(f"\nError running examples: {str(e)}")
        import traceback
        traceback.print_exc()
