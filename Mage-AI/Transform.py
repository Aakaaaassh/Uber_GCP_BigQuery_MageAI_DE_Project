import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    df = data 

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    df = df.drop_duplicates().reset_index(drop = True)
    df['ID'] = df.index

    datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
    datetime_dim['tpep_pickup_datetime'] = datetime_dim['tpep_pickup_datetime']
    datetime_dim['pickup_minute'] = datetime_dim['tpep_pickup_datetime'].dt.minute
    datetime_dim['pickup_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pickup_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pickup_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pickup_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pickup_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday

    datetime_dim['tpep_dropoff_datetime'] = datetime_dim['tpep_dropoff_datetime']
    datetime_dim['dropoff_minute'] = datetime_dim['tpep_dropoff_datetime'].dt.minute
    datetime_dim['dropoff_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['dropoff_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['dropoff_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['dropoff_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['dropoff_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday

    datetime_dim['datetime_id'] = datetime_dim.index

    datetime_dim = datetime_dim[['datetime_id','tpep_pickup_datetime','pickup_minute','pickup_hour','pickup_day','pickup_month','pickup_year','pickup_weekday','tpep_dropoff_datetime','dropoff_minute','dropoff_hour','dropoff_day','dropoff_month','dropoff_year','dropoff_weekday']]

    pickup_dim = df[['pickup_longitude','pickup_latitude']].reset_index(drop=True)
    pickup_dim['pickup_location_id'] = pickup_dim.index
    pickup_dim = pickup_dim[['pickup_location_id','pickup_longitude','pickup_latitude']]

    dropoff_dim = df[['dropoff_longitude','dropoff_latitude']].reset_index(drop=True)
    dropoff_dim['dropoff_location_id'] = dropoff_dim.index
    dropoff_dim = dropoff_dim[['dropoff_location_id','dropoff_longitude','dropoff_latitude']]

    rate_code_type = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }

    ratecode_dim = df[['RatecodeID']].reset_index(drop=True)
    ratecode_dim['rate_code_id'] = ratecode_dim.index
    ratecode_dim['rate_code_name'] = ratecode_dim['RatecodeID'].map(rate_code_type)
    ratecode_dim = ratecode_dim[['rate_code_id','RatecodeID','rate_code_name']]

    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }

    payment_type_dim = df[['payment_type']].reset_index(drop = True)
    payment_type_dim['payment_id'] = payment_type_dim.index
    payment_type_dim['payment_name'] = payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_id','payment_type','payment_name']]

    trip_details_dim = df[['passenger_count','trip_distance']].reset_index(drop=True)
    trip_details_dim['trip_id'] = trip_details_dim.index
    trip_details_dim = trip_details_dim[['trip_id','passenger_count','trip_distance']]


    total_amount_dim = df[['fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount']].reset_index(drop=True)
    total_amount_dim['total_amount_id'] = total_amount_dim.index
    total_amount_dim = total_amount_dim.merge(payment_type_dim, left_on='total_amount_id', right_on='payment_id')
    total_amount_dim = total_amount_dim[['total_amount_id','payment_id','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount']]
    

    fact_table = df[['VendorID','store_and_fwd_flag']].reset_index(drop=True)
    fact_table['ID'] = fact_table.index
    fact_table = fact_table.merge(datetime_dim, left_on='ID', right_on='datetime_id') \
                            .merge(pickup_dim, left_on='ID', right_on='pickup_location_id') \
                            .merge(dropoff_dim, left_on='ID', right_on='dropoff_location_id') \
                            .merge(trip_details_dim, left_on='ID', right_on='trip_id') \
                            .merge(ratecode_dim, left_on='ID', right_on='rate_code_id') \
                            .merge(total_amount_dim, left_on='ID', right_on='total_amount_id') \
                            [['ID','VendorID','datetime_id','pickup_location_id','dropoff_location_id','trip_id','rate_code_id','store_and_fwd_flag','payment_id','total_amount_id']]
    return {"datetime_dim":datetime_dim.to_dict(orient="dict"),
    "trip_details_dim":trip_details_dim.to_dict(orient="dict"),
    "pickup_dim":pickup_dim.to_dict(orient="dict"),
    "dropoff_dim":dropoff_dim.to_dict(orient="dict"),
    "ratecode_dim":ratecode_dim.to_dict(orient="dict"),
    "total_amount_dim":total_amount_dim.to_dict(orient="dict"),
    "payment_type_dim":payment_type_dim.to_dict(orient="dict"),
    "fact_table":fact_table.to_dict(orient="dict")}


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'