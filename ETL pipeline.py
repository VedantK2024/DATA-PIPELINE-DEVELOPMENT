"""
ETL Pipeline for Data Preprocessing, Transformation, and Loading
================================================================

This script provides a comprehensive ETL pipeline with the following features:
- Multiple data source support (CSV, Excel, JSON, SQL databases)
- Data validation and quality checks
- Data cleaning and preprocessing
- Feature engineering and transformation
- Multiple output formats
- Logging and error handling
- Configuration-based execution



"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
import json
import warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ETLPipeline:
    """
    A comprehensive ETL Pipeline for data preprocessing, transformation, and loading.
    """
    
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize the ETL Pipeline.
        
        Args:
            config: Configuration dictionary for pipeline settings
        """
        self.config = config or {}
        self.data = None
        self.metadata = {
            'pipeline_start': datetime.now(),
            'source_info': {},
            'transformations_applied': [],
            'data_quality_metrics': {}
        }
        logger.info("ETL Pipeline initialized")
    
    # ==================== EXTRACT METHODS ====================
    
    def extract_csv(self, filepath: str, **kwargs) -> pd.DataFrame:
        """
        Extract data from CSV file.
        
        Args:
            filepath: Path to CSV file
            **kwargs: Additional arguments for pd.read_csv()
        
        Returns:
            DataFrame with extracted data
        """
        try:
            logger.info(f"Extracting data from CSV: {filepath}")
            df = pd.read_csv(filepath, **kwargs)
            self.metadata['source_info']['type'] = 'CSV'
            self.metadata['source_info']['path'] = filepath
            self.metadata['source_info']['rows'] = len(df)
            self.metadata['source_info']['columns'] = len(df.columns)
            logger.info(f"Successfully extracted {len(df)} rows and {len(df.columns)} columns")
            return df
        except Exception as e:
            logger.error(f"Error extracting CSV: {str(e)}")
            raise
    
    def extract_excel(self, filepath: str, sheet_name: Union[str, int] = 0, **kwargs) -> pd.DataFrame:
        """
        Extract data from Excel file.
        
        Args:
            filepath: Path to Excel file
            sheet_name: Sheet name or index
            **kwargs: Additional arguments for pd.read_excel()
        
        Returns:
            DataFrame with extracted data
        """
        try:
            logger.info(f"Extracting data from Excel: {filepath}, Sheet: {sheet_name}")
            df = pd.read_excel(filepath, sheet_name=sheet_name, **kwargs)
            self.metadata['source_info']['type'] = 'Excel'
            self.metadata['source_info']['path'] = filepath
            self.metadata['source_info']['sheet'] = sheet_name
            self.metadata['source_info']['rows'] = len(df)
            self.metadata['source_info']['columns'] = len(df.columns)
            logger.info(f"Successfully extracted {len(df)} rows and {len(df.columns)} columns")
            return df
        except Exception as e:
            logger.error(f"Error extracting Excel: {str(e)}")
            raise
    
    def extract_json(self, filepath: str, **kwargs) -> pd.DataFrame:
        """
        Extract data from JSON file.
        
        Args:
            filepath: Path to JSON file
            **kwargs: Additional arguments for pd.read_json()
        
        Returns:
            DataFrame with extracted data
        """
        try:
            logger.info(f"Extracting data from JSON: {filepath}")
            df = pd.read_json(filepath, **kwargs)
            self.metadata['source_info']['type'] = 'JSON'
            self.metadata['source_info']['path'] = filepath
            self.metadata['source_info']['rows'] = len(df)
            self.metadata['source_info']['columns'] = len(df.columns)
            logger.info(f"Successfully extracted {len(df)} rows and {len(df.columns)} columns")
            return df
        except Exception as e:
            logger.error(f"Error extracting JSON: {str(e)}")
            raise
    
    def extract_sql(self, query: str, connection_string: str) -> pd.DataFrame:
        """
        Extract data from SQL database.
        
        Args:
            query: SQL query to execute
            connection_string: Database connection string
        
        Returns:
            DataFrame with extracted data
        """
        try:
            from sqlalchemy import create_engine
            logger.info(f"Extracting data from SQL database")
            engine = create_engine(connection_string)
            df = pd.read_sql(query, engine)
            self.metadata['source_info']['type'] = 'SQL'
            self.metadata['source_info']['query'] = query
            self.metadata['source_info']['rows'] = len(df)
            self.metadata['source_info']['columns'] = len(df.columns)
            logger.info(f"Successfully extracted {len(df)} rows and {len(df.columns)} columns")
            return df
        except Exception as e:
            logger.error(f"Error extracting from SQL: {str(e)}")
            raise
    
    # ==================== DATA QUALITY & VALIDATION ====================
    
    def validate_data(self, df: pd.DataFrame, rules: Optional[Dict] = None) -> Dict:
        """
        Validate data against specified rules.
        
        Args:
            df: DataFrame to validate
            rules: Dictionary of validation rules
        
        Returns:
            Dictionary with validation results
        """
        logger.info("Validating data quality")
        validation_results = {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'missing_values': df.isnull().sum().to_dict(),
            'duplicate_rows': df.duplicated().sum(),
            'data_types': df.dtypes.astype(str).to_dict(),
            'issues': []
        }
        
        # Check for missing values
        missing_pct = (df.isnull().sum() / len(df) * 100).to_dict()
        for col, pct in missing_pct.items():
            if pct > 50:
                validation_results['issues'].append(f"Column '{col}' has {pct:.2f}% missing values")
        
        # Check for duplicate rows
        if validation_results['duplicate_rows'] > 0:
            validation_results['issues'].append(
                f"Found {validation_results['duplicate_rows']} duplicate rows"
            )
        
        # Custom validation rules
        if rules:
            for rule_name, rule_func in rules.items():
                try:
                    result = rule_func(df)
                    validation_results[rule_name] = result
                except Exception as e:
                    validation_results['issues'].append(f"Rule '{rule_name}' failed: {str(e)}")
        
        self.metadata['data_quality_metrics'] = validation_results
        logger.info(f"Validation complete. Found {len(validation_results['issues'])} issues")
        return validation_results
    
    def generate_data_profile(self, df: pd.DataFrame) -> Dict:
        """
        Generate a comprehensive data profile.
        
        Args:
            df: DataFrame to profile
        
        Returns:
            Dictionary with profiling information
        """
        logger.info("Generating data profile")
        profile = {
            'shape': df.shape,
            'memory_usage': df.memory_usage(deep=True).sum() / 1024**2,  # MB
            'columns': {},
            'summary_stats': df.describe(include='all').to_dict()
        }
        
        for col in df.columns:
            col_info = {
                'dtype': str(df[col].dtype),
                'missing_count': int(df[col].isnull().sum()),
                'missing_percentage': float(df[col].isnull().sum() / len(df) * 100),
                'unique_count': int(df[col].nunique()),
                'unique_percentage': float(df[col].nunique() / len(df) * 100)
            }
            
            if pd.api.types.is_numeric_dtype(df[col]):
                col_info['mean'] = float(df[col].mean()) if not df[col].isnull().all() else None
                col_info['std'] = float(df[col].std()) if not df[col].isnull().all() else None
                col_info['min'] = float(df[col].min()) if not df[col].isnull().all() else None
                col_info['max'] = float(df[col].max()) if not df[col].isnull().all() else None
            
            profile['columns'][col] = col_info
        
        return profile
    
    # ==================== TRANSFORM METHODS ====================
    
    def handle_missing_values(self, df: pd.DataFrame, strategy: str = 'drop', 
                             fill_value: Any = None, columns: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Handle missing values in the dataset.
        
        Args:
            df: DataFrame to process
            strategy: 'drop', 'fill', 'mean', 'median', 'mode', 'forward_fill', 'backward_fill'
            fill_value: Value to fill if strategy is 'fill'
            columns: Specific columns to apply the strategy to
        
        Returns:
            DataFrame with missing values handled
        """
        logger.info(f"Handling missing values with strategy: {strategy}")
        df = df.copy()
        cols = columns or df.columns.tolist()
        
        if strategy == 'drop':
            df = df.dropna(subset=cols)
        elif strategy == 'fill':
            df[cols] = df[cols].fillna(fill_value)
        elif strategy == 'mean':
            for col in cols:
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(df[col].mean())
        elif strategy == 'median':
            for col in cols:
                if pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = df[col].fillna(df[col].median())
        elif strategy == 'mode':
            for col in cols:
                if not df[col].mode().empty:
                    df[col] = df[col].fillna(df[col].mode()[0])
        elif strategy == 'forward_fill':
            df[cols] = df[cols].fillna(method='ffill')
        elif strategy == 'backward_fill':
            df[cols] = df[cols].fillna(method='bfill')
        
        self.metadata['transformations_applied'].append(f"Missing values handled: {strategy}")
        return df
    
    def remove_duplicates(self, df: pd.DataFrame, subset: Optional[List[str]] = None, 
                         keep: str = 'first') -> pd.DataFrame:
        """
        Remove duplicate rows.
        
        Args:
            df: DataFrame to process
            subset: Columns to consider for duplicates
            keep: 'first', 'last', or False to drop all duplicates
        
        Returns:
            DataFrame without duplicates
        """
        logger.info("Removing duplicate rows")
        df = df.copy()
        initial_rows = len(df)
        df = df.drop_duplicates(subset=subset, keep=keep)
        removed = initial_rows - len(df)
        logger.info(f"Removed {removed} duplicate rows")
        self.metadata['transformations_applied'].append(f"Duplicates removed: {removed} rows")
        return df
    
    def handle_outliers(self, df: pd.DataFrame, columns: List[str], 
                       method: str = 'iqr', threshold: float = 1.5) -> pd.DataFrame:
        """
        Handle outliers in numerical columns.
        
        Args:
            df: DataFrame to process
            columns: Columns to check for outliers
            method: 'iqr' or 'zscore'
            threshold: IQR multiplier or z-score threshold
        
        Returns:
            DataFrame with outliers handled
        """
        logger.info(f"Handling outliers using {method} method")
        df = df.copy()
        
        for col in columns:
            if not pd.api.types.is_numeric_dtype(df[col]):
                continue
            
            if method == 'iqr':
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - threshold * IQR
                upper_bound = Q3 + threshold * IQR
                df[col] = df[col].clip(lower=lower_bound, upper=upper_bound)
            
            elif method == 'zscore':
                mean = df[col].mean()
                std = df[col].std()
                df[col] = df[col].clip(lower=mean - threshold * std, 
                                       upper=mean + threshold * std)
        
        self.metadata['transformations_applied'].append(f"Outliers handled: {method}")
        return df
    
    def normalize_columns(self, df: pd.DataFrame, columns: List[str], 
                         method: str = 'minmax') -> pd.DataFrame:
        """
        Normalize numerical columns.
        
        Args:
            df: DataFrame to process
            columns: Columns to normalize
            method: 'minmax' or 'zscore'
        
        Returns:
            DataFrame with normalized columns
        """
        logger.info(f"Normalizing columns using {method} method")
        df = df.copy()
        
        for col in columns:
            if not pd.api.types.is_numeric_dtype(df[col]):
                continue
            
            if method == 'minmax':
                min_val = df[col].min()
                max_val = df[col].max()
                df[col] = (df[col] - min_val) / (max_val - min_val)
            
            elif method == 'zscore':
                mean = df[col].mean()
                std = df[col].std()
                df[col] = (df[col] - mean) / std
        
        self.metadata['transformations_applied'].append(f"Normalization: {method}")
        return df
    
    def encode_categorical(self, df: pd.DataFrame, columns: List[str], 
                          method: str = 'label') -> pd.DataFrame:
        """
        Encode categorical variables.
        
        Args:
            df: DataFrame to process
            columns: Columns to encode
            method: 'label' or 'onehot'
        
        Returns:
            DataFrame with encoded columns
        """
        logger.info(f"Encoding categorical columns using {method} method")
        df = df.copy()
        
        if method == 'label':
            from sklearn.preprocessing import LabelEncoder
            for col in columns:
                if col in df.columns:
                    le = LabelEncoder()
                    df[col] = le.fit_transform(df[col].astype(str))
        
        elif method == 'onehot':
            df = pd.get_dummies(df, columns=columns, prefix=columns)
        
        self.metadata['transformations_applied'].append(f"Categorical encoding: {method}")
        return df
    
    def create_datetime_features(self, df: pd.DataFrame, date_column: str) -> pd.DataFrame:
        """
        Create datetime-based features from a date column.
        
        Args:
            df: DataFrame to process
            date_column: Name of the date column
        
        Returns:
            DataFrame with new datetime features
        """
        logger.info(f"Creating datetime features from {date_column}")
        df = df.copy()
        
        df[date_column] = pd.to_datetime(df[date_column])
        df[f'{date_column}_year'] = df[date_column].dt.year
        df[f'{date_column}_month'] = df[date_column].dt.month
        df[f'{date_column}_day'] = df[date_column].dt.day
        df[f'{date_column}_dayofweek'] = df[date_column].dt.dayofweek
        df[f'{date_column}_quarter'] = df[date_column].dt.quarter
        df[f'{date_column}_is_weekend'] = df[date_column].dt.dayofweek.isin([5, 6]).astype(int)
        
        self.metadata['transformations_applied'].append(f"Datetime features created from {date_column}")
        return df
    
    def apply_custom_transformation(self, df: pd.DataFrame, 
                                   transformation_func, **kwargs) -> pd.DataFrame:
        """
        Apply a custom transformation function.
        
        Args:
            df: DataFrame to process
            transformation_func: Custom function to apply
            **kwargs: Additional arguments for the transformation function
        
        Returns:
            Transformed DataFrame
        """
        logger.info("Applying custom transformation")
        df = transformation_func(df, **kwargs)
        self.metadata['transformations_applied'].append("Custom transformation applied")
        return df
    
    # ==================== LOAD METHODS ====================
    
    def load_csv(self, df: pd.DataFrame, filepath: str, **kwargs) -> None:
        """
        Load data to CSV file.
        
        Args:
            df: DataFrame to save
            filepath: Output file path
            **kwargs: Additional arguments for df.to_csv()
        """
        try:
            logger.info(f"Loading data to CSV: {filepath}")
            df.to_csv(filepath, index=False, **kwargs)
            logger.info(f"Successfully saved {len(df)} rows to {filepath}")
        except Exception as e:
            logger.error(f"Error loading to CSV: {str(e)}")
            raise
    
    def load_excel(self, df: pd.DataFrame, filepath: str, sheet_name: str = 'Sheet1', 
                   **kwargs) -> None:
        """
        Load data to Excel file.
        
        Args:
            df: DataFrame to save
            filepath: Output file path
            sheet_name: Name of the sheet
            **kwargs: Additional arguments for df.to_excel()
        """
        try:
            logger.info(f"Loading data to Excel: {filepath}")
            df.to_excel(filepath, sheet_name=sheet_name, index=False, **kwargs)
            logger.info(f"Successfully saved {len(df)} rows to {filepath}")
        except Exception as e:
            logger.error(f"Error loading to Excel: {str(e)}")
            raise
    
    def load_json(self, df: pd.DataFrame, filepath: str, orient: str = 'records', 
                  **kwargs) -> None:
        """
        Load data to JSON file.
        
        Args:
            df: DataFrame to save
            filepath: Output file path
            orient: JSON orientation
            **kwargs: Additional arguments for df.to_json()
        """
        try:
            logger.info(f"Loading data to JSON: {filepath}")
            df.to_json(filepath, orient=orient, **kwargs)
            logger.info(f"Successfully saved {len(df)} rows to {filepath}")
        except Exception as e:
            logger.error(f"Error loading to JSON: {str(e)}")
            raise
    
    def load_sql(self, df: pd.DataFrame, table_name: str, connection_string: str,
                 if_exists: str = 'replace', **kwargs) -> None:
        """
        Load data to SQL database.
        
        Args:
            df: DataFrame to save
            table_name: Name of the table
            connection_string: Database connection string
            if_exists: 'fail', 'replace', or 'append'
            **kwargs: Additional arguments for df.to_sql()
        """
        try:
            from sqlalchemy import create_engine
            logger.info(f"Loading data to SQL table: {table_name}")
            engine = create_engine(connection_string)
            df.to_sql(table_name, engine, if_exists=if_exists, index=False, **kwargs)
            logger.info(f"Successfully saved {len(df)} rows to {table_name}")
        except Exception as e:
            logger.error(f"Error loading to SQL: {str(e)}")
            raise
    
    def load_parquet(self, df: pd.DataFrame, filepath: str, **kwargs) -> None:
        """
        Load data to Parquet file (efficient columnar storage).
        
        Args:
            df: DataFrame to save
            filepath: Output file path
            **kwargs: Additional arguments for df.to_parquet()
        """
        try:
            logger.info(f"Loading data to Parquet: {filepath}")
            df.to_parquet(filepath, index=False, **kwargs)
            logger.info(f"Successfully saved {len(df)} rows to {filepath}")
        except Exception as e:
            logger.error(f"Error loading to Parquet: {str(e)}")
            raise
    
    # ==================== PIPELINE EXECUTION ====================
    
    def run_pipeline(self, source_config: Dict, transform_config: List[Dict], 
                    load_config: Dict) -> Dict:
        """
        Run the complete ETL pipeline.
        
        Args:
            source_config: Configuration for data extraction
            transform_config: List of transformation configurations
            load_config: Configuration for data loading
        
        Returns:
            Metadata dictionary with pipeline execution details
        """
        try:
            logger.info("=" * 60)
            logger.info("Starting ETL Pipeline")
            logger.info("=" * 60)
            
            # EXTRACT
            source_type = source_config.pop('type')
            if source_type == 'csv':
                self.data = self.extract_csv(**source_config)
            elif source_type == 'excel':
                self.data = self.extract_excel(**source_config)
            elif source_type == 'json':
                self.data = self.extract_json(**source_config)
            elif source_type == 'sql':
                self.data = self.extract_sql(**source_config)
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
            
            # Validate initial data
            validation_results = self.validate_data(self.data)
            
            # TRANSFORM
            for transform in transform_config:
                operation = transform.pop('operation')
                logger.info(f"Applying transformation: {operation}")
                
                if operation == 'handle_missing':
                    self.data = self.handle_missing_values(self.data, **transform)
                elif operation == 'remove_duplicates':
                    self.data = self.remove_duplicates(self.data, **transform)
                elif operation == 'handle_outliers':
                    self.data = self.handle_outliers(self.data, **transform)
                elif operation == 'normalize':
                    self.data = self.normalize_columns(self.data, **transform)
                elif operation == 'encode_categorical':
                    self.data = self.encode_categorical(self.data, **transform)
                elif operation == 'datetime_features':
                    self.data = self.create_datetime_features(self.data, **transform)
                else:
                    logger.warning(f"Unknown operation: {operation}")
            
            # LOAD
            load_type = load_config.pop('type')
            if load_type == 'csv':
                self.load_csv(self.data, **load_config)
            elif load_type == 'excel':
                self.load_excel(self.data, **load_config)
            elif load_type == 'json':
                self.load_json(self.data, **load_config)
            elif load_type == 'sql':
                self.load_sql(self.data, **load_config)
            elif load_type == 'parquet':
                self.load_parquet(self.data, **load_config)
            else:
                raise ValueError(f"Unsupported load type: {load_type}")
            
            # Finalize metadata
            self.metadata['pipeline_end'] = datetime.now()
            self.metadata['pipeline_duration'] = (
                self.metadata['pipeline_end'] - self.metadata['pipeline_start']
            ).total_seconds()
            self.metadata['final_shape'] = self.data.shape
            
            logger.info("=" * 60)
            logger.info("ETL Pipeline Completed Successfully")
            logger.info(f"Duration: {self.metadata['pipeline_duration']:.2f} seconds")
            logger.info(f"Final data shape: {self.metadata['final_shape']}")
            logger.info("=" * 60)
            
            return self.metadata
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise
    
    def save_metadata(self, filepath: str = 'pipeline_metadata.json') -> None:
        """Save pipeline metadata to JSON file."""
        metadata_copy = self.metadata.copy()
        # Convert datetime objects to strings
        metadata_copy['pipeline_start'] = str(metadata_copy['pipeline_start'])
        if 'pipeline_end' in metadata_copy:
            metadata_copy['pipeline_end'] = str(metadata_copy['pipeline_end'])
        
        with open(filepath, 'w') as f:
            json.dump(metadata_copy, f, indent=2, default=str)
        logger.info(f"Metadata saved to {filepath}")


# ==================== EXAMPLE USAGE ====================

def example_usage():
    """
    Example demonstrating how to use the ETL Pipeline.
    """
    
    # Initialize pipeline
    pipeline = ETLPipeline()
    
    # Example configuration
    source_config = {
        'type': 'csv',
        'filepath': 'input_data.csv'
    }
    
    transform_config = [
        {
            'operation': 'handle_missing',
            'strategy': 'mean',
            'columns': ['age', 'income']
        },
        {
            'operation': 'remove_duplicates'
        },
        {
            'operation': 'handle_outliers',
            'columns': ['income'],
            'method': 'iqr',
            'threshold': 1.5
        },
        {
            'operation': 'encode_categorical',
            'columns': ['category'],
            'method': 'label'
        },
        {
            'operation': 'normalize',
            'columns': ['age', 'income'],
            'method': 'minmax'
        }
    ]
    
    load_config = {
        'type': 'csv',
        'filepath': 'output_data.csv'
    }
    
    # Run pipeline
    try:
        metadata = pipeline.run_pipeline(source_config, transform_config, load_config)
        pipeline.save_metadata()
        print("\nPipeline executed successfully!")
        print(f"Transformations applied: {len(metadata['transformations_applied'])}")
    except Exception as e:
        print(f"Pipeline failed: {str(e)}")


if __name__ == "__main__":
    print("ETL Pipeline Script")
    print("=" * 60)
    print("This is a comprehensive ETL pipeline framework.")
    print("Modify the example_usage() function to run with your data.")
    print("=" * 60)
    
    # Uncomment to run example
    # example_usage()
