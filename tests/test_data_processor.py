import pandas as pd
import pytest
from src.data_processor import DataProcessor

@pytest.fixture
def processor():
    return DataProcessor()

def test_clean_phone_numbers_removes_non_numeric(processor):
    df = pd.DataFrame({'phone_number': ['123-456-7890', '(+34) 600 123 456', 'abc123def']})
    cleaned_df = processor.clean_phone_numbers(df.copy())
    assert cleaned_df['phone_number'].iloc[0] == '1234567890'
    assert cleaned_df['phone_number'].iloc[1] == '34600123456' # Assuming Spanish locale logic
    assert cleaned_df['phone_number'].iloc[2] == '123'

def test_clean_phone_numbers_handles_nan_and_empty(processor):
    df = pd.DataFrame({'phone_number': [None, '', '123456789']})
    cleaned_df = processor.clean_phone_numbers(df.copy())
    assert cleaned_df['phone_number'].iloc[0] == ''
    assert cleaned_df['phone_number'].iloc[1] == ''
    assert cleaned_df['phone_number'].iloc[2] == '123456789'

def test_validate_phone_numbers_basic_length(processor):
    df = pd.DataFrame({'phone_number': ['123456789', '12345678', '123456789012345', '1234567890123456']})
    validated_df = processor.validate_phone_numbers(df.copy())
    assert validated_df['is_valid_phone'].iloc[0] == True
    assert validated_df['is_valid_phone'].iloc[1] == False
    assert validated_df['is_valid_phone'].iloc[2] == True
    assert validated_df['is_valid_phone'].iloc[3] == False

def test_deduplicate_data(processor):
    df = pd.DataFrame({
        'customer_id': [1, 2, 3, 4],
        'phone_number': ['111', '222', '111', '333']
    })
    deduplicated_df = processor.deduplicate_data(df.copy())
    assert len(deduplicated_df) == 3
    assert '111' in deduplicated_df['phone_number'].values
    assert '222' in deduplicated_df['phone_number'].values
    assert '333' in deduplicated_df['phone_number'].values
    # Ensure the first occurrence is kept
    assert deduplicated_df[deduplicated_df['phone_number'] == '111']['customer_id'].iloc[0] == 1

def test_full_processing_flow(processor):
    df = pd.DataFrame({
        'customer_id': [1, 2, 3, 4, 5],
        'customer_name': ['A', 'B', 'C', 'D', 'E'],
        'email': ['a@a.com', 'b@b.com', 'c@c.com', 'd@d.com', 'e@e.com'],
        'phone_number': ['123-456-789', 'invalid', '987654321', '123-456-789', None]
    })
    processed_df = processor.process(df.copy())

    # Expected results after cleaning, validation, and deduplication
    # 123-456-789 -> 123456789 (valid, duplicated)
    # invalid -> invalid (not valid)
    # 987654321 -> 987654321 (valid)
    # 123-456-789 -> 123456789 (duplicate, should be removed)
    # None -> '' (not valid)

    assert len(processed_df) == 3 # 123456789 (first), invalid, 987654321
    assert processed_df['phone_number'].iloc[0] == '123456789'
    assert processed_df['is_valid_phone'].iloc[0] == True

    assert processed_df['phone_number'].iloc[1] == 'invalid'
    assert processed_df['is_valid_phone'].iloc[1] == False

    assert processed_df['phone_number'].iloc[2] == '987654321'
    assert processed_df['is_valid_phone'].iloc[2] == True
