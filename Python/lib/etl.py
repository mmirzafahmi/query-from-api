import gcsfs
import logging
import pandas as pd
from pyarrow import parquet


def extract_data_from_storage(url):
    """
    This function simply get parquet files from cloud storage and return it as dataframe.

    parameters
    ----------
    url : string
        gsutil URI

    returns
    -------
    dataframe
        combine all splitted parquet files into a single dataframe.
    """
    # Connect to Google Cloud Storage.
    fs = gcsfs.GCSFileSystem()
    # Assuming your parquet files start with `part-` prefix
    files = ["gs://" + path for path in fs.glob(url + "part-*")]
    # Combine all parquet files
    ds = parquet.ParquetDataset(files[:1], filesystem=fs)
    # Read and convert to pandas dataframe
    df = ds.read().to_pandas()
    return df


def preprocess(df_ga, df_td, visitor_id):
    """
    Preprocess dataframe to get some insights by filtering and extracting.

    parameters
    ----------
    df_ga : Pandas Dataframe
        Dataframe contains Google analytics dataset.
    df_td : Pandas Dataframe
        Dataframe contains transactional dataset.
    visitor_id : string
        String contains fullVisitorId generated by Google Analytics.

    returns
    -------
    integer
        fullVisitorId generated by Google Analytics
    boolean
        flag if visitor changed their address or not
    boolean
        flag if visitor placed their order or not
    boolean
        flag if order was delivered or not
    string
        Type of application which user's used.
    """

    # Initiate empty list to store array data
    data = []

    # Filter dataframe based on their fullVisitorId
    df = df_ga[df_ga['fullvisitorid'] == visitor_id].reset_index(drop=True)

    # Flatten array inside dataframe into dictionary
    for hits in df['hit']:
        hits = dict(enumerate(hits.flatten(), 1))
        for k, v in hits.items():
            data.append(v)

    # Convert stored array data into dataframe
    df_hit = pd.DataFrame(data)

    # Flag user who placed their order based on their event action
    df_hit['isPlacedOrder'] = df_hit['eventAction'].apply(lambda ea: True if ea == 'transaction' else False)
    # Flag user who changed their address based on their event action
    df_hit['isAddressChange'] = df_hit['eventAction'].apply(
        lambda ea: True if ea in ['address_update.clicked', 'Change Location', 'other_location.clicked',
                                  'address_update.submitted'] else False
    )
    # Extract the necessary insight
    is_address_changed = max(df_hit['isAddressChange'].values)
    is_placed_order = max(df_hit['isPlacedOrder'].values)
    application_type = df['operatingSystem'].values[0]
    is_order_delivered = False

    # Get transaction id
    if len(df_hit[df_hit['transactionId'].notnull()]['transactionId'].values) > 0:
        tid = df_hit[df_hit['transactionId'].notnull()]['transactionId'].values[0]
        # Filter data based on their transaction id
        df_td = df_td[df_td['frontendOrderId'] == tid]
        # Flag user whose order were delivered

        if len(df_td['geopointDropoff'].notnull().values) > 0:
            is_order_delivered = True if df_td['geopointDropoff'].notnull().values[0] else False

        return (int(visitor_id), is_address_changed, is_placed_order, is_order_delivered, application_type)
    else:
        return (int(visitor_id), is_address_changed, is_placed_order, is_order_delivered, application_type)

