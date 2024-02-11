import pandas as pd


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    months = [f'{month:02d}' for month in range(1, 13)]

    # root url path
    root_path = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{}.parquet'

    # generate list of full urls
    urls = [root_path.format(mon) for mon in months]

    # read all tables into a list of dataframes
    dataframes = [pd.read_parquet(url) for url in urls]

    # concatenate all dataframes into one
    combined_df = pd.concat(dataframes, ignore_index=True)

    # change data type
    combined_df['lpep_pickup_date'] = combined_df['lpep_pickup_datetime'].dt.date
    combined_df['lpep_dropoff_date'] = combined_df['lpep_dropoff_datetime'].dt.date

    return combined_df
    

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'



from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    bucket_name = 'de-zoomcamp-xiangivyli'
    object_key = 'ny_green_taxi_data_2022.parquet'

    GoogleCloudStorage.with_config(ConfigFileLoader(config_path, config_profile)).export(
        df,
        bucket_name,
        object_key,
    )