if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    print("Rows with zero passengers or zero trip distance:",
      ((data['passenger_count'] == 0) | (data['trip_distance'] == 0)).sum())

    print("Unique values of VendorID:",
      data['VendorID'].unique())

    #add columns
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    #rename columns in Camel Case to Snake Case
    data.columns = [col.replace('ID', '_id').lower() 
                   if 'ID' in col else col.lower() for col in data.columns]

    return data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output['vendor_id'].notnull().all() 
    assert output['passenger_count'].isin([0]).sum() == 0
    assert output['trip_distance'].isin([0]).sum() == 0

