import pandas as pd
import numpy as np
import sendgrid
from sendgrid.helpers.mail import Mail, Email, Content
def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    bucket_name = file['bucket']
    file_name = file['name']
    # print(f"Processing file: {file['name']}.")
    # print(f"Bucket: {file['bucket']}.")
    path = 'gs://{}/{}'.format(bucket_name,file_name)
    df = pd.read_csv(path)
    print("read bucket data!")
    exception_df = exception_finder(df)
    # send_for_review(exception_df)
    print(email(exception_df))

def exception_finder(df):
  row_with_error = {}
  invalid_contract_numbers_payments_indexes = invalid_contract_numbers_payments(df)
  if type(invalid_contract_numbers_payments_indexes) != bool:
    for index in invalid_contract_numbers_payments_indexes:
        row_with_error.setdefault(index, []).append('INVALID PAYMENTS CONTRACT NUMBER')

  invalid_contract_numbers_purchase_orders_indexes = invalid_contract_numbers_purchase_orders(df)
  if type(invalid_contract_numbers_purchase_orders_indexes) != bool:
    for index in invalid_contract_numbers_purchase_orders_indexes:
        row_with_error.setdefault(index, []).append('INVALID PURCHASE ORDER CONTRACT NUMBER')

  mismatched_contract_numbers_indexes = matching_contract_numbers(df)
  if type(mismatched_contract_numbers_indexes) != bool:
    for index in mismatched_contract_numbers_indexes:
        row_with_error.setdefault(index, []).append('MISMATCHED CONTRACT NUMBERS IN PAYMENT AND PURCHASE ORDER')

  mismatched_invoice_and_payment_date_indexes = invoice_date_and_payment_date_matching(df)
  if type(mismatched_invoice_and_payment_date_indexes) != bool:
    for index in mismatched_invoice_and_payment_date_indexes:
        row_with_error.setdefault(index, []).append('MISMATCHED INVOICE AND PAYMENT DATES')

  invalid_invoice_number_indexes = invalid_invoice_numbers(df)
  if type(invalid_invoice_number_indexes) != bool:
    for index in invalid_invoice_number_indexes:
        row_with_error.setdefault(index, []).append('INVALID INVOICE NUMBER')

  mismatched_fiscal_year_indexes = fiscal_year_check(df)
  if type(mismatched_fiscal_year_indexes) != bool:
    for index in mismatched_fiscal_year_indexes:
        row_with_error.setdefault(index, []).append('MISMATCHED FISCAL YEARS')


  mismatched_created_date_and_ordered_date_indexes = created_date_and_ordered_date_matching(df)
  if type(mismatched_created_date_and_ordered_date_indexes) != bool:
    for index in mismatched_created_date_and_ordered_date_indexes:
        row_with_error.setdefault(index, []).append('MISMATCHED CREATE AND ORDERED DATE')

  exception_df = pd.DataFrame.from_dict(row_with_error, orient='index')
  return exception_df

def matching_contract_numbers(df):
    mismatched_rows_index = df.index[df['CONTRACTNUMBER_Payment'] != df['CONTRACTNUMBER_purchase_Orders']]
    if mismatched_rows_index.empty:
        return False
    else:
        return mismatched_rows_index

def invalid_contract_numbers_payments(df):
    invalid_payments_rows_index = df.index[(df['CONTRACTNUMBER_Payment'].isnull())]
    if invalid_payments_rows_index.empty:
        return False
    else:
        return invalid_payments_rows_index

def invalid_contract_numbers_purchase_orders(df):
    invalid_rows_index = df.index[(df['CONTRACTNUMBER_purchase_Orders'].isnull())]
    if invalid_rows_index.empty:
        return False
    else:
        return invalid_rows_index
        
def invalid_invoice_numbers(df):
    invalid_invoice_rows_index = df.index[(df['INVOICENUMBER_Payment'].isnull())]
    if invalid_invoice_rows_index.empty:
        return False
    else:
        return invalid_invoice_rows_index

def invoice_date_and_payment_date_matching(df):
    df['PAYMENTDATE_Payment'] = pd.to_datetime(df['PAYMENTDATE_Payment']).dt.tz_localize(None)
    df['INVOICEDATE_Payment'] = pd.to_datetime(df['INVOICEDATE_Payment']).dt.tz_localize(None)
    error_row_index = df.index[df['INVOICEDATE_Payment'] > df['PAYMENTDATE_Payment']]
    if error_row_index.empty:
        return False
    else:
        return error_row_index

def fiscal_year_check(df):
    dfFY = pd.DataFrame(df['FISCALYEAR_Payment'].astype(int).astype(str))
    dfFY['FY_START'] = dfFY['FISCALYEAR_Payment'].apply(lambda x: str(int(x) - 1) + '-10-01')
    dfFY['FY_END'] = dfFY['FISCALYEAR_Payment'].apply(lambda x: x + '-09-30')

    dfFY['FY_START'] = pd.to_datetime(dfFY['FY_START'], format='%Y-%m-%d').dt.tz_localize(None)  # Remove timezone info
    dfFY['FY_END'] = pd.to_datetime(dfFY['FY_END'], format='%Y-%m-%d').dt.tz_localize(None)  # Remove timezone info

    mismatched_fiscal_year_row_indexes = df.index[(dfFY['FY_END'] < df['PAYMENTDATE_Payment']) & (dfFY['FY_START'] > df['PAYMENTDATE_Payment'])]
    if mismatched_fiscal_year_row_indexes.empty:
        return False
    else:
        return mismatched_fiscal_year_row_indexes

def created_date_and_ordered_date_matching(df):
    df['CREATEDATE_purchase_Orders'] = pd.to_datetime(df['CREATEDATE_purchase_Orders']).dt.tz_localize(None)
    df['ORDEREDDATE_purchase_Orders'] = pd.to_datetime(df['ORDEREDDATE_purchase_Orders']).dt.tz_localize(None)
    problem_row_index = df.index[df['CREATEDATE_purchase_Orders'] > df['ORDEREDDATE_purchase_Orders']]
    if problem_row_index.empty:
        return False
    else:
        return problem_row_index

def send_for_review(df):
  """Sends an email notification with invoice exception details.

  Args:
      df (pandas.DataFrame): The DataFrame containing exception data.
  """


  # Recipient email address (replace with your desired recipient)
  recipient_email = "fordeeplab@gmail.com"

  # Email message body
  message_body = f"""
  An exceptions have been identified in invoice processing!

  Regards,
  Invoice Processing System
  """


  print(f"Sent email notification for invoice exceptions!.")

def email(df):
    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import Mail, Email
    from python_http_client.exceptions import HTTPError

    sg = SendGridAPIClient('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')

    html_content = df.to_html()

    message = Mail(
        to_emails="fordeeplab@gmail.com",
        from_email=Email('aipr.gcp@gmail.com', "Aditya Kejariwal"),
        subject="Invoice Processing Exceptions",
        html_content=html_content
        )
    message.add_bcc("pikapool444@gmail.com")

    try:
        response = sg.send(message)
        return f"email.status_code={response.status_code}"
        #expected 202 Accepted

    except HTTPError as e:
        return e.message