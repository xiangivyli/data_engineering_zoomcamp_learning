import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    root_path = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/'
    file_names = ['green_tripdata_2020-10.csv.gz', 
                  'green_tripdata_2020-11.csv.gz', 
                  'green_tripdata_2020-12.csv.gz']
    
    
    green_taxi_dtypes = {
                    'VendorID': pd.Int64Dtype(),
                    'passenger_coount': pd.Int64Dtype(),
                    'trip_distance':float,
                    'RatecodeID':pd.Int64Dtype(),
                    'store_and_fwd_flag':str,
                    'PULocationID':pd.Int64Dtype(),
                    'DOLocationID':pd.Int64Dtype(),
                    'payment_type':pd.Int64Dtype(),
                    'fare_amount':float,
                    'extra':float,
                    'mta_tax':float,
                    'tip_amount':float,
                    'tolls_amount':float,
                    'improvement_surcharge':float,
                    'totoal_amount':float,
                    'congestion_surcharge':float
         
                }

    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']


    # Empty DataFrame to hold concatenated data
    concatenated_df = pd.DataFrame()

    #Loop over the file names, read then concatenate

    for file_name in file_names:
        # Build the full URL for the current file
        file_url = f"{root_path}{file_name}"

    
        df = pd.read_csv(file_url, sep=",", compression="gzip", dtype=green_taxi_dtypes, 
            parse_dates=parse_dates)

        concatenated_df = pd.concat([concatenated_df, df], ignore_index=True)

    return concatenated_df
    

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
