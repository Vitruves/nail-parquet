#!/usr/bin/env python3
import pandas as pd
import numpy as np
from pathlib import Path

def create_test_data():
    """Create test data similar to the user's structure"""
    np.random.seed(42)
    n_rows = 1000
    
    # Create data similar to the user's structure
    data = {
        'source': np.random.choice(['source_A', 'source_B', 'source_C'], n_rows),
        'product': [f'product_{i}' for i in range(n_rows)],
        'overall_rating': np.random.randint(1, 6, n_rows),
        'condition': np.random.choice(['condition_1', 'condition_2', 'condition_3'], n_rows),
        'comment': [f'comment_{i}' for i in range(n_rows)],
        'sex': np.random.choice(['M', 'F'], n_rows),
        'duration': np.random.randint(1, 365, n_rows),
        'date': pd.date_range('2023-01-01', periods=n_rows, freq='D')[:n_rows],
        'age': np.random.randint(18, 80, n_rows),
        'username': [f'user_{i}' for i in range(n_rows)],
        'patient_type': np.random.choice(['type_A', 'type_B'], n_rows),
        'INN': np.random.choice(['INN_1', 'INN_2', 'INN_3'], n_rows),  # Note: uppercase INN
        'prediction': np.random.choice(['positive', 'negative', 'neutral'], n_rows),  # String column
        'predicted_label': np.random.randint(0, 3, n_rows),
    }
    
    df = pd.DataFrame(data)
    
    # Create directory if it doesn't exist
    output_dir = Path('test_data')
    output_dir.mkdir(exist_ok=True)
    
    # Save as parquet
    output_file = output_dir / 'test_reviews.parquet'
    df.to_parquet(output_file, index=False)
    print(f"Created test data: {output_file}")
    print(f"Columns: {list(df.columns)}")
    print(f"Data types:")
    for col, dtype in df.dtypes.items():
        print(f"  {col}: {dtype}")
    
    return output_file

if __name__ == '__main__':
    create_test_data() 