import pandas as pd 

# Convert to int
def convert_to_int(value):
    if pd.isna(value) or value == '':
        return ''
    return int(value)