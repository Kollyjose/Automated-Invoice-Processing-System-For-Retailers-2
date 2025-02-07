#LATEST:

import pandas as pd
import ast
import re
import numpy as np
from google.cloud import storage

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
    #print(df.head())

    #list of columns to drop once preprocessing is done 
    drop_list=[]

    #FIRST PREPROCESSING: TOTAL COLUMN VALIDATION
    if "total" in df.columns.tolist():      
     df['total'].fillna('[]',inplace=True)
     
     #convert to a list
     df['total'] = df['total'].apply(ast.literal_eval)      

     
     #function to extract the numeric value from total:
     def detect_num(input_list):
          for x in input_list:
               match = re.search(r'\d+\.\d+',x)
               if match:
                    float_value = float(match.group())
                    return float_value  # Output: 461.05
               else:
                    return np.NaN
          return np.NaN

          #apply the function    
     df['total_numeric']=df['total'].apply(detect_num)
     #print(df['total_numeric'][0])
     print("total_validation_successful!")
     drop_list.append("total")

    #2.QUANTITY COLUMN

    if "quantity" in df.columns.tolist():
     def extract_float(input_list):
          # Use regular expression to find floating-point numbers
          values = re.findall(r'\d+\.\d+', "".join(input_list))
          # Convert the strings to float values
          result_list = [float(value) for value in values]
          if len(result_list)==0:
               return np.NaN
          return result_list

     df['quantity'].fillna('[]',inplace=True)
     
     #convert to a list if applicable:
     try:
          df['quantity'] = df['quantity'].apply(ast.literal_eval)
     except:
          pass

     #apply the function        
     df['quantity_numeric']=df['quantity'].apply(extract_float)

     print("quantity column preprocessed!")
     #print(df['quantity_numeric'][0])
     drop_list.append('quantity')

    #3. TAX COLUMN

    if "tax" in df.columns.tolist():

     #split tax into 2 columns - tax_percentage and tax_amount

          #fill null values
     df['tax'].fillna("[]",inplace=True)

          #regex to find float number followed by percentage
     def tax_percent(input_list):
          if not input_list:
               return np.NaN

          pattern = r'\b\d+\.\d+%'
          match = re.findall(pattern, "".join(input_list))
          if match:
               return match
          return np.NaN

          #regex to find float number after a percentage sign
     def tax_amount(input_list):
          pattern = r'%.*?(\d+\.\d+)'
          match = re.search(pattern, "".join(input_list))
          if match:
               matched_number = match.group(1)
               return matched_number
          else:
               return np.NaN
          
          #convert to a list if applicable:
     try:
          df['tax'] = df['tax'].apply(ast.literal_eval)
     except:
          pass
          
     df['tax_percentage']=df['tax'].apply(tax_percent)
     df['tax_amount']=df['tax'].apply(tax_amount)

     print("tax column preprocessed!")
     #print(df["tax_percentage"][0])
     #print(df['tax_amount'][0])
     drop_list.append('tax')

    #4. GETTING BUYER INFORMATION

    if "buyer" in df.columns.tolist():
     def get_buyer(input_list):
          pattern = r'Buyer\s*:\s*([A-Za-z\s]+).*?\n'    
          match = re.search(pattern, "".join(input_list))
          if match:
               matched_text = match.group(1)
               return(matched_text.strip())
          else:
               return np.NaN

          #fill na:
     df['buyer'].fillna("[]",inplace=True)
     
          #convert to a list if applicable:
     try:
          df['buyer'] = df['buyer'].apply(ast.literal_eval)
     except:
          pass

     df["buyer_name"]=df['buyer'].apply(get_buyer)
     #print(df['buyer_name'][1])
     print("buyer name processing successful!")
     drop_list.append('buyer')

    # 5. Bill To

    def get_billto(input_list):
     pattern = r'Bill\s*to\s*:\s*([A-Za-z\s]+).*?\n'    
     match = re.search(pattern, "".join(input_list))
     if match:
          matched_text = match.group(1)
          return(matched_text.strip())
     else:
          pattern = r'BILL_TO\s*:\s*([A-Za-z\s]+).*?\n'
          match = re.search(pattern, "".join(input_list))
          if match:
               matched_text = match.group(1)
               return(matched_text.strip())
          return np.NaN
     return np.NaN

    if "bill to" in df.columns.tolist(): 

     #fill na:
     df['bill to'].fillna("[]",inplace=True)
          
     #convert to a list if applicable:
     try:
          df['bill to'] = df['bill to'].apply(ast.literal_eval)
     except:
          pass

     df["bill to name"]=df['bill to'].apply(get_billto)
     print("Bill To Preprocessing done!")
     drop_list.append('bill to')

    if "bill_to" in df.columns.tolist():
     df['bill_to'].fillna("[]",inplace=True)
     try:
          df['bill_to'] = df['bill_to'].apply(ast.literal_eval)
     except:
          pass

     df["bill_to name"]=df['bill_to'].apply(get_billto)
     print("bill_to preprocessing done!")
     drop_list.append('bill_to')


    #6. Ship To Preprocessing

    if "ship_to" in df.columns.tolist():
          

     def get_shipto(input_list):
          pattern = r'‘SHIP_TO:\n([A-Za-z\s]+).*?\n'    
          match = re.search(pattern, "".join(input_list))
          if match:
               matched_text = match.group(1)
               return matched_text.strip()
          else:
               return np.NaN

     df['ship_to'].fillna('[]',inplace=True)
     
     #convert to a list if applicable:
     try:
          df['ship_to'] = df['ship_to'].apply(ast.literal_eval)
     except:
          pass

     df["ship_to_name"]=df['ship_to'].apply(get_shipto)

     print("Ship to Preprocessing done!")
     drop_list.append('ship_to')

    #7. Invoice number:
    if 'invoice number' in df.columns.tolist():
     def clean_invoice_number(invoice):
          if pd.isnull(invoice):
               return ''
          invoice = ''.join(e for e in str(invoice) if e.isalnum())
          invoice = invoice.strip()
          return invoice
     
     # Fill NaN values with null strings
     df['invoice number'] = df['invoice number'].fillna('')

     # Apply the cleaning function to the 'invoice number' column
     df['invoice_number_cleaned'] = df['invoice number'].apply(clean_invoice_number)

     # Replace invoice numbers containing 'inyourpaymentnotes' with an empty string
     df['invoice_number_cleaned'] = df['invoice_number_cleaned'].apply(lambda x: '' if 'inyourpaymentnotes' in x else x)
     df['invoice_number_cleaned']=df['invoice_number_cleaned'].replace("",np.NaN)

     drop_list.append("invoice number")
     print("Invoice Number Cleaned!")

    #8. Preprocess invoice id column
    if "invoice_id" in df.columns.tolist():
     def preprocess_invoice_id(invoice_id):
          if isinstance(invoice_id, str):
               cleaned_invoice_id = invoice_id.strip("[]").strip().replace("'", "")
               return cleaned_invoice_id
          else:
               return np.NaN

     def preprocess_invoice_ids(dataframe, column_name):
          cleaned_invoice_ids = dataframe[column_name].apply(lambda x: preprocess_invoice_id(x))
          dataframe['cleaned_invoice_id'] = cleaned_invoice_ids
          return dataframe

     df = preprocess_invoice_ids(df, 'invoice id')
     drop_list.append("invoice id")
     print("Invoice ID Processed!")

    #9. Getting Company Name from Address Column:
 
    if "address" in df.columns.tolist():
     def get_company_name(input_list):
          input_str = "".join(input_list)
          input_str=input_str.replace("‘","")
          input_str=input_str.replace("\n","")
          parts = input_str.split('Address:')
          #print(parts)
          if len(parts) > 1:
               company_name = parts[0].strip()
               company_address=parts[1].strip()
               company_name = company_name.replace("\n","").replace("[","").replace("]","").replace("'","")
               return company_name
          elif len(parts)>0:
               company_address = parts[0].strip()
               company_address=company_address.replace("Address","")
               return np.NaN
          else:
               return np.NaN

     df["company_name"]=df['address'].apply(get_company_name)
     df['company_name']=df['company_name'].replace("",np.NaN)
     drop_list.append("address")
     print("Company Name extracted from Address!")

    #10. Preprocess and validate the PO Number column:
    if 'po number' in df.columns.tolist():
     #fill NA values:
     df['po number'].fillna('[]',inplace=True)
     
     #convert to a list if applicable:
     try:
          df['po number'] = df['po number'].apply(ast.literal_eval)
     except:
          pass
     
     def get_ponumber(input_list):
          pattern=r'([\d]+)'
          match=re.search(pattern,"".join(input_list))
          if match:
               matched_text=match.group(1)
               return(int(matched_text.strip()))
          else:
               return np.NaN
    
     df['po number']=df['po number'].apply(get_ponumber)

     print("PO Number Preprocessed!")

    #11. Discount Extraction
    
    def get_discount_numeric(input_list):
     pattern=r'(\d+\.\d+)'
     match=re.search(pattern,"".join(input_list))
     if match:
          matched_text=match.group(1)
          return(float(matched_text.strip()))
     else:
          return np.NaN

    if "discount" in df.columns.tolist():
     #fill NA values:
     df['discount'].fillna('[]',inplace=True)
     
     #convert to a list if applicable:
     try:
          df['discount'] = df['discount'].apply(ast.literal_eval)
     except:
          pass

     df['discount']=df['discount'].apply(get_discount_numeric)
     print("Discount extracted!")

    #12. Sub Total Extraction

    def get_subtotal_currency(input_list):
     pattern = r'([^0-9.-:,]+)'
     match=re.search(pattern,"".join(input_list))
     if match:
          matched_text=match.group(1)
          return(matched_text.strip())
     else:
          return np.NaN

    if 'sub_total' in df.columns.tolist():
     def get_subtotal_numeric(input_list):
          pattern=r'(\d+\.\d+)'
          match=re.search(pattern,"".join(input_list))
          if match:
               matched_text=match.group(1)
               return(float(matched_text.strip()))
          else:
               return np.NaN

     #fill NA values:
     df['sub_total'].fillna('[]',inplace=True)
    
     #convert to a list if applicable:
     try:
          df['sub_total'] = df['sub_total'].apply(ast.literal_eval)
     except:
          pass
     
     df['sub_total_value']=df['sub_total'].apply(get_discount_numeric)


    
     df["sub_total_currency"]=df['sub_total'].apply(get_subtotal_currency)

     df['sub_total_currency']=df['sub_total_currency'].replace("§", 'USD')
     df['sub_total_currency']=df['sub_total_currency'].replace("", np.NaN)
     df['sub_total_currency']=df['sub_total_currency'].replace("$", 'USD')
     #print(df["sub_total_currency"].unique())


     print("Extracted Value and Currency from Subtotal!")
     drop_list.append("sub_total")


    #13. Balance Extraction

    if 'balance' in df.columns.tolist():
     # we can apply the same preprocessing as sub_total column here.
     #fill NA values:
     df['balance'].fillna('[]',inplace=True)
     
     #convert to a list if applicable:
     try:
          df['balance'] = df['balance'].apply(ast.literal_eval)
     except:
          pass
     
     df['balance_value']=df['balance'].apply(get_discount_numeric)
     df["balance_currency"]=df['balance'].apply(get_subtotal_currency)
     drop_list.append('balance')


     df['balance_currency']=df['balance_currency'].replace("§", 'USD')
     df['balance_currency']=df['balance_currency'].replace("", np.NaN)
     df['balance_currency']=df['balance_currency'].replace("$", 'USD')
     #print(df['balance_currency'].unique())

     print("Balance value and currency extracted!")

    #14. Balance_Due Extraction

    if 'balance_due' in df.columns.tolist():
     # we can apply the same preprocessing as sub_total column here.
     #fill NA values:
     df['balance_due'].fillna('[]',inplace=True)
     
     #convert to a list if applicable:
     try:
          df['balance_due'] = df['balance_due'].apply(ast.literal_eval)
     except:
          pass
     
     df['balance_due_value']=df['balance_due'].apply(get_discount_numeric)
     df["balance_due_currency"]=df['balance_due'].apply(get_subtotal_currency)
     drop_list.append('balance_due')


     df['balance_due_currency']=df['balance_due_currency'].replace("§", 'USD')
     df['balance_due_currency']=df['balance_due_currency'].replace("", np.NaN)
     df['balance_due_currency']=df['balance_due_currency'].replace("$", 'USD')
     #print(df['balance_due_currency'].unique())

     print("balance_due value and currency extracted!")

    #15. Bank info processing
    def bank_processor(df,col_name):
     df[col_name]=df[col_name].fillna('[]')
     df[col_name]=df[col_name].apply(ast.literal_eval)
     df[col_name]=df[col_name].apply(lambda x:"".join(x))
     df[col_name]=df[col_name].apply(lambda x:x.replace('_',"").replace(" ","").replace("—",""))
     return df

    if "branch name" in df.columns.tolist():
     df=bank_processor(df,"branch name")

    if "bank name" in df.columns.tolist():
     df=bank_processor(df,'bank name')

    if "bank account number" in df.columns.tolist():
     df=bank_processor(df,"bank account number")

    if "bank swift code" in df.columns.tolist():
     df=bank_processor(df,"bank swift code")

     print("bank info processed!")

    #16. GSTIN deletion
    if "gstin" in df.columns.tolist():
     df.drop(columns=['gstin'],inplace=True)

    #17. Site Cleaning
    if "site" in df.columns.tolist():
     df=bank_processor(df,"site")
     print("Site data cleaned!")

    #18. Tel Cleaning
    if "tel" in df.columns.tolist():
     
     def format_tel(input_list):
          for x in input_list:
               digits = re.findall(r'\d', x)
               if len(digits) == 10:
                    extracted_digits = ''.join(digits)
                    return extracted_digits
               return np.NaN
          return np.NaN

     #convert tel to list:
     df['tel']=df['tel'].fillna('[]')
     try:
          df['tel']=df['tel'].apply(ast.literal_eval)
     except:
          pass
     df['tel']=df['tel'].apply(format_tel)
     print("Tel Column processed!")

     #19. Email Column Cleaning:
     
     if "email" in df.columns.tolist():

          def get_email(input_list):
               if len(input_list)==0:
                    return np.NaN,np.NaN
               elif len(input_list)==1:
                    return input_list[0],np.NaN
               else:
                    return input_list[0],input_list[1]

          df['email']=df['email'].fillna('[]')

          try:
               df['email']=df['email'].apply(ast.literal_eval)
          except:
               pass
          
          df['email_1'], df['email_2'] = zip(*df['email'].apply(get_email))
          drop_list.append("email")
          print("email column processed!")

    
    #20. Date Formatting
    def check_for_date(input_list):
     pattern = r'^\d{2}-[A-Za-z]{3}-\d{4}$'
     matching_strings = [s for s in input_list if re.match(pattern, s)]
     return matching_strings

    def extract_from_column(input_list):
     if len(input_list)>2:
          print(input_list)
          return np.NaN,np.NaN
     if len(input_list)==1:
          return input_list[0],np.NaN
     elif len(input_list)==0:
          return np.NaN,np.NaN
     else:
          return input_list[0],input_list[1]

    def convert_to_datetime(date_str):
     try:
          return pd.to_datetime(date_str, format='%d-%b-%Y')
     except ValueError:
          return np.NaN

    if "date" in df.columns.tolist():
     df['date']=df['date'].replace(np.NaN,'[]')
     
     try:
          df['date']=df['date'].apply(ast.literal_eval)
     except:
          pass
     
     df['date']=df['date'].apply(check_for_date)
     df['date_1'], df['date_2'] = zip(*df['date'].apply(extract_from_column))
     df['date_1'] = df['date_1'].apply(convert_to_datetime)
     df['date_2'] = df['date_2'].apply(convert_to_datetime)

     drop_list.append("date")
     print("Date column cleaned!")

    if "due date" in df.columns.tolist():
     df['due date']=df['due date'].replace(np.NaN,'[]')
     try:
          df['due date']=df['due date'].apply(ast.literal_eval)
     except:
          pass
     
     df['due date']=df['due date'].apply(check_for_date)
     df['due date']=df['due date'].apply(lambda x:"".join(x))
     df['due date'] = df['due date'].apply(convert_to_datetime)

     drop_list.append("due date")
     print("due date column cleaned!")
     
    
    #Loading the Cleaned Data

    #drop uneeded columns:
    if "invoice" in df.columns.tolist():
     drop_list.append("invoice")
    if "Unnamed: 0" in df.columns.tolist():
     drop_list.append("Unnamed: 0")


    
    df.drop(columns=drop_list,inplace=True)
    print("columns dropped!")



    #set new file name:
    file_name=file_name.replace(".csv","")
    processed_csv_name="{}_cleaned".format(file_name)
    #processed_csv_name.replace('.csv',"") 

    # Convert DataFrame back to CSV
    csv_content = df.to_csv()

    print("df exported to CSV")

    # Upload CSV to destination bucket
    destination_bucket_name = "ocr-cleaned-data"
    destination_file_name = "{}.csv".format(processed_csv_name)
    storage_client = storage.Client()
    destination_bucket = storage_client.bucket(destination_bucket_name)
    destination_blob = destination_bucket.blob(destination_file_name)
    destination_blob.upload_from_string(csv_content, content_type='text/csv')
    print("Cleaned CSV file exported to bucket!")
