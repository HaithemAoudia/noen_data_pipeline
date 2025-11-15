import requests
from requests.auth import HTTPBasicAuth
import gspread  
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe, get_as_dataframe
import time
import pandas as pd
import os




API_EMAIL = os.getenv("API_EMAIL")
API_KEY = os.getenv("API_KEY")

google_cred = {
    "type": os.getenv("TYPE"),
    "project_id": os.getenv("PROJECT_ID"),
    "private_key_id": os.getenv("PRIVATE_KEY_ID"),
    "private_key": os.getenv("PRIVATE_KEY").replace('\\n', '\n'),
    "client_email": os.getenv("CLIENT_EMAIL"),
    "client_id": os.getenv("CLIENT_ID"),
    "auth_uri": os.getenv("AUTH_URI"),
    "token_uri": os.getenv("TOKEN_URI"),
    "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
    "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL"),
    "universe_domain": os.getenv("UNIVERSE_DOMAIN")
}





def fetch_data(type, offset, limit):
    url = f"https://api.oneup.com/v1/{type}?limit={limit}&offset={offset}&sort=-created_at"

    response = requests.get(url, auth=HTTPBasicAuth(API_EMAIL, API_KEY), verify=False)

    if response.status_code == 200:
        print("Success")
        data = response.json()
        return data
    else:
        error = f"Error {response.status_code}: {response.text}"
        return error
    

def transform_invoices(data):

    records = []

    for invoice in data:
        # Extract invoice-level fields
        po_number = invoice.get("po_number")
        delivery_status = invoice.get("delivery_status")
        invoice_status = invoice.get("invoice_status")
        sent = invoice.get("sent")
        sent_at = invoice.get("sent_at")
        paid = invoice.get("paid")
        unpaid = invoice.get("unpaid")
        customer_id = invoice.get("customer_id")
        customer_name = invoice.get("customer", {}).get("name")
        date = invoice.get("date")
        invoice_id = invoice.get("user_code")
        
        billing = invoice.get("billing_address", {})
        country = billing.get("country")
        city = billing.get("city")
        postal_code = billing.get("postal_code")
        street_line = billing.get("street_line1")
        total_amount = invoice.get("total")
        
        # Each invoice may have multiple installments
        for installment in invoice.get("installments", []):
            due_date = installment.get("due_date")
            amount = installment.get("amount")
            outstanding_amount = installment.get("outstanding_amount")
            created_at = installment.get("created_at")
            updated_at = installment.get("updated_at")
            
            # Each invoice may have multiple order lines
            for line in invoice.get("order_lines", []):
                record = {
                    "invoice_id": invoice_id,
                    "date": date,
                    "due_date": due_date,
                    "amount": amount,
                    "outstanding_amount": outstanding_amount,
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "po_number": po_number,
                    "invoice_status": invoice_status,
                    "delivery_status": delivery_status,
                    "sent": sent,
                    "sent_at": sent_at,
                    "paid": paid,
                    "unpaid": unpaid,
                    "customer_id": customer_id,
                    "customer_name": customer_name,
                    "country": country,
                    "city": city,
                    "postal_code": postal_code,
                    "street_line": street_line,
                    "order_line_id": line.get("id"),
                    "item_id": line.get("item_id"),
                    "item_description": line.get("description"),
                    "quantity": line.get("quantity"),
                    "unit_price": line.get("unit_price_wt"),
                    "total_order_line": line.get("total"),
                    "subtotal": invoice.get("subtotal"),
                    "tax_amount": invoice.get("tax_amount"),
                    "total": total_amount,
                    "source": "OneUp"
                }
                records.append(record)

    # Create DataFrame
    df = pd.DataFrame(records)

    return df


def transform_customers(data):
    records = []

    for customer in data:
        # --- Top-level fields ---
        customer_id = customer.get("id")
        full_name = customer.get("full_name")
        created_at = customer.get("created_at")
        updated_at = customer.get("updated_at")
        email = customer.get("email")
        opt_in_email = customer.get("opt_in_email")
        industry = customer.get("industry")
        rating = customer.get("rating")

        # --- Nested: address ---
        address = customer.get("address", {}) or {}
        address_line1 = address.get("street_line1")
        postal_code = str(address.get("postal_code")) if address.get("postal_code") is not None else None
        city = address.get("city")
        country = address.get("country")

        # --- Nested: payment terms ---
        payment_terms = customer.get("payment_terms", {}) or {}
        payment_terms_name = payment_terms.get("name")
        payment_terms_id = payment_terms.get("id")

        # --- Nested: sales tax ---
        sales_tax = customer.get("sales_tax", {}) or {}
        sales_tax_id = sales_tax.get("id")
        sales_tax_name = sales_tax.get("name")
        sales_tax_enabled = sales_tax.get("enabled")

        # --- Nested: price family ---
        price_family = customer.get("price_family", {}) or {}
        price_family_standard = price_family.get("name")

        # --- Nested: accounting account ---
        accounting_account = customer.get("accounting_account", {}) or {}
        accounting_account_id = accounting_account.get("id")

        # --- Append flattened record ---
        records.append({
            "id": customer_id,
            "full_name": full_name,
            "created_at": created_at,
            "updated_at": updated_at,
            "address_line1": address_line1,
            "postal_code": postal_code,
            "city": city,
            "country": country,
            "email": email,
            "payment_terms_name": payment_terms_name,
            "payment_terms_id": payment_terms_id,
            "sales_tax_id": sales_tax_id,
            "sales_tax_name": sales_tax_name,
            "sales_tax_enabled": sales_tax_enabled,
            "opt_in_email": opt_in_email,
            "price_family_standard": price_family_standard,
            "industry": industry,
            "rating": rating,
            "accounting_account_id": accounting_account_id
        })

    # --- Create DataFrame AFTER the loop ---
    df_customers = pd.DataFrame(records)
    return df_customers

def transform_products(data):

    records = []

    for item in data:
        # --- Basic / top-level fields ---
        item_id = item.get("id")
        name = item.get("name")
        type_ = item.get("type")
        item_number = item.get("item_number")
        description = item.get("description")
        sales_price = item.get("sales_price")
        purchase_price = item.get("purchase_price")
        created_at = item.get("created_at")
        updated_at = item.get("updated_at")

        # --- Nested: unit ---
        unit_data = item.get("unit", {}) or {}
        unit_id = unit_data.get("id")
        unit_created_at = unit_data.get("created_at")
        unit_updated_at = unit_data.get("updated_at")

        # --- Nested: item family ---
        family_data = item.get("item_family", {}) or {}
        item_family_name = family_data.get("name")

        # --- Nested: COGS account ---
        cogs_data = item.get("cogs_account", {}) or {}
        cogs_account_name = cogs_data.get("name")
        cogs_account_id = cogs_data.get("id")

        # --- Nested: income account ---
        income_data = item.get("income_account", {}) or {}
        income_account_name = income_data.get("name")
        income_account_id = income_data.get("id")

        # --- Nested: purchase tax ---
        purchase_tax_data = item.get("purchase_tax", {}) or {}
        purchase_tax_name = purchase_tax_data.get("name")
        purchase_tax_rate = purchase_tax_data.get("rate")
        purchase_tax_id = purchase_tax_data.get("id")

        # --- Nested: sales tax ---
        sales_tax_data = item.get("sales_tax", {}) or {}
        sales_tax_name = sales_tax_data.get("name")
        sales_tax_rate = sales_tax_data.get("rate")
        sales_tax_id = sales_tax_data.get("id")

        records.append({
        "id": item_id,
        "created_at": created_at,
        "updated_at": updated_at,
        "unit_id": unit_id,
        "name": name,
        "type": type_,
        "unit_created_at": unit_created_at,
        "unit_updated_at": unit_updated_at,
        "sales_price": sales_price,
        "item_number": item_number,
        "description": description,
        "item_family_name": item_family_name,
        "purchase_price": purchase_price,
        "cogs_account_name": cogs_account_name,
        "cogs_account_id": cogs_account_id,
        "income_account_name": income_account_name,
        "income_account_id": income_account_id,
        "purchase_tax_name": purchase_tax_name,
        "purchase_tax_rate": purchase_tax_rate,
        "purchase_tax_id": purchase_tax_id,
        "sales_tax_name": sales_tax_name,
        "sales_tax_rate": sales_tax_rate,
        "sales_tax_id": sales_tax_id
    })
        # Create DataFrame
    df_items = pd.DataFrame(records)
    return df_items


def load_data(sheet_name, nk, api_type, method):
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(google_cred, scopes=scopes)
    client = gspread.authorize(creds)

    sheet_id = os.getenv("SHEET_ID")
    workbook = client.open_by_key(sheet_id)
    sheet = workbook.worksheet(sheet_name)
    offset = 0
    batch_size = 100
    df_current = get_as_dataframe(workbook.worksheet(sheet_name))
    current_nks = df_current[nk].dropna().unique().tolist()
    
    if method == 'Append':
        next_row = len(sheet.get_all_values()) + 1

        while True:
            try:
                # fetch data from API
                json = fetch_data(type=api_type, limit="100", offset=f"{offset}")

                if not json:
                    print("No data returned — finished.")
                    break
                
                # transform json to pandas df
                if api_type == 'invoices':
                    df = transform_invoices(json)
                elif api_type == 'items':
                    df = transform_products(json)
                elif api_type == 'customers':
                    df = transform_customers(json)

                # Add source column
                df['source'] = 'OneUp'
                
                # Identify extra columns in df_current that are not in df
                extra_cols = [col for col in df_current.columns if col not in df.columns]
                
                # Add extra columns to df with NaN values
                for col in extra_cols:
                    df[col] = pd.NA

                # filter out any rows that are not in the google sheet by cross checking the natural key
                df = df[~df[nk].isin(current_nks)]

                if df.empty:
                    print(f"No more new data found at offset {offset}. Stopping.")
                    break

                # append new rows to sheet
                set_with_dataframe(sheet, df, row=next_row, include_column_header=False)

                next_row += len(df)
                offset += batch_size
                current_nks.extend(df[nk].tolist())

                print(f"Uploaded {len(df)} rows (offset={offset})")
                time.sleep(0.3)

            except Exception as e:
                print(f"⚠️ Error at offset {offset}: {e}")
                time.sleep(2)
                continue
                
    elif method == 'overwrite':
        # Collect all data from API first
        all_data = []
        
        while True:
            try:
                # fetch data from API
                json = fetch_data(type=api_type, limit="100", offset=f"{offset}")

                if not json:
                    print("No data returned — finished fetching.")
                    break
                
                # transform json to pandas df
                if api_type == 'invoices':
                    df = transform_invoices(json)
                elif api_type == 'items':
                    df = transform_products(json)
                elif api_type == 'customers':
                    df = transform_customers(json)

                if df.empty:
                    print(f"No more data found at offset {offset}. Stopping fetch.")
                    break

                all_data.append(df)
                offset += batch_size
                print(f"Fetched {len(df)} rows (offset={offset})")
                time.sleep(0.3)

            except Exception as e:
                print(f"⚠️ Error at offset {offset}: {e}")
                time.sleep(2)
                continue
        
        # Combine all fetched data
        if all_data:
            df_new = pd.concat(all_data, ignore_index=True)
            
            # Add source column to new data
            df_new['source'] = 'OneUp'
            
            # Identify extra columns in df_current that are not in df_new
            extra_cols = [col for col in df_current.columns if col not in df_new.columns]
            
            # Create a mapping of natural key to extra column values from df_current
            if extra_cols:
                df_current_extra = df_current[[nk] + extra_cols].copy()
                # Merge to preserve extra column values for matching natural keys
                df_new = df_new.merge(df_current_extra, on=nk, how='left')
            
            # For rows that don't have matching natural keys, add extra columns with NaN
            for col in extra_cols:
                if col not in df_new.columns:
                    df_new[col] = pd.NA
            
            # Keep rows from df_current that are not in df_new (if any completely new columns were added manually)
            df_current_only = df_current[~df_current[nk].isin(df_new[nk])]
            
            # Combine updated new data with any remaining current data
            if not df_current_only.empty:
                # Ensure df_current_only has 'source' column
                if 'source' not in df_current_only.columns:
                    df_current_only['source'] = 'OneUp'
                df_updated = pd.concat([df_new, df_current_only], ignore_index=True)
            else:
                df_updated = df_new
            
            # Ensure column order matches df_current
            final_cols = list(df_current.columns) + [col for col in df_updated.columns if col not in df_current.columns]
            df_updated = df_updated[final_cols]
            
            # Clear sheet and upload the complete updated dataframe
            sheet.clear()
            set_with_dataframe(sheet, df_updated, include_column_header=True)
            
            print(f"✅ Overwrite complete: {len(df_updated)} total rows uploaded")
        else:
            print("No new data fetched from API")


# Run Invoices
# print("Loading Invoices Data")
# load_data(sheet_name="OneUp - Invoices", nk="order_line_id", api_type="invoices", method='Append')

# Run Products
print("Loading Products Data")
load_data(sheet_name="OneUp - Products", nk="id", api_type="items", method='overwrite')

# Run Customers
# print("Loading Customer Data")
# load_data(sheet_name="OneUp - Customers", nk="id", api_type="customers", method='overwrite')






"""
1. Create a natural key (NK) as a unique identifier for each row
2. When the pipeline is executed, we will keep retrieving the lastest data and create a natural key  
3. Check if the natural key exists in the google sheets, as long does not exists we keep loading the data
4. Once we find an entry that exists we exit the data loading



NK Definition: order_line_id """











