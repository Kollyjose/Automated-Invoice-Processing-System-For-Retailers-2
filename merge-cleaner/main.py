from google.cloud import storage
import pandas as pd 
import datetime as dt 


def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """

    file = event
    bucket_name=file['bucket']
    file_name=file['name']
    print(f"Processing file: {file['name']}.")
    print(f"Bucket: {file['bucket']}.")
    path='gs://{}/{}'.format(bucket_name, file_name)
    df = pd.read_csv(path)

    def format_dates(df):
      # Convert date fields from string to datetime objects
      df['PAYMENTDATE_Payment'] = pd.to_datetime(df['PAYMENTDATE_Payment'])
      df['INVOICEDATE_Payment'] = pd.to_datetime(df['INVOICEDATE_Payment'])
      df['ESTPAYMENTDATE_Payment'] = pd.to_datetime(df['ESTPAYMENTDATE_Payment'])
      df['ORDEREDDATE_purchase_Orders'] = pd.to_datetime(df['ORDEREDDATE_purchase_Orders'])
      df['CREATEDATE_purchase_Orders'] = pd.to_datetime(df['CREATEDATE_purchase_Orders'])
      return df
    
    columns_to_drop = ['VOUCHERNUMBER_Payment', 'PAYMENTNUMBER_Payment', 'RECORDUPDATEDDATE_Payment','RECORDCREATED_Payment', 'DCS_REC_CRT_DTTM_Payment', 'DCS_LAST_MOD_DTTM_Payment',
                       'OBJECTID_Payment', 'TRANSACTION_CODE_Payment',
                       'AGENCYCODE_purchase_Orders', 'REQUISTIONNUMBER_purchase_Orders',
                       'DCS_REC_CRT_DTTM_purchase_Orders', 'DCS_LAST_MOD_DTTM_purchase_Orders',
                       'DCS_LAST_MOD_DTTM_purchase_Orders', 'AGENCY_ACRONYM_purchase_Orders','AGENCYCODE_Payment','COMMODITYCODE_purchase_Orders','FISCALYEAR_purchase_Orders','OBJECT_ID_purchase_Orders']

    df=format_dates(df)
    df.drop(columns=columns_to_drop,inplace=True)
    
    df = df.rename(columns={'AGENCY_ACRONYM_Payment': 'AGENCY_ACRONYM'})
    df['insertion_time']=pd.to_datetime('now').replace(microsecond=0)
    
    
    
    processed_csv_name="{}_cleaned".format(file_name)
    processed_csv_name.replace('.csv',"")
 
    # Convert DataFrame back to CSV
    csv_content = df.to_csv()
 
    print("df exported to CSV")
 
    # Upload CSV to destination bucket
    destination_bucket_name = "pass-data-store-final"
    destination_file_name = "{}.csv".format(processed_csv_name)
    storage_client = storage.Client()
    destination_bucket = storage_client.bucket(destination_bucket_name)
    destination_blob = destination_bucket.blob(destination_file_name)
    destination_blob.upload_from_string(csv_content, content_type='text/csv')
    print("Cleaned CSV file exported to bucket!")
