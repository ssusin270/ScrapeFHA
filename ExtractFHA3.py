# # -*- coding: utf-8 -*-
# """
# Created on Wed Oct 29 12:34:11 2025

# @author: scott
# """

import tabula
import pandas as pd
import os
import re
from pathlib import Path
from datetime import datetime
from datetime import date

def extract_date_from_filename(filename):
    """
    Extract date from FHA report filename.
    Examples: FHAProdReport_Dec2021.pdf -> 2021-12-01
    """
    # Try various filename patterns
    patterns = [
        r'(\w{3})(\d{4})',  # Dec2021
        r'(\w{4})(\d{4})',  # June2021
        r'(\w{3})_(\d{4})', # Dec_2021
        r'([a-zA-Z]+)(\d{4})'    # December2021
    ]
    
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            month_str = match.group(1)
            year = match.group(2)
            
            # Convert month name to number
            month_map = {
                'jan': 1, 'january': 1,
                'feb': 2, 'february': 2,
                'mar': 3, 'march': 3,
                'apr': 4, 'april': 4,
                'may': 5,
                'jun': 6, 'june': 6,
                'jul': 7, 'july': 7,
                'aug': 8, 'august': 8,
                'sep': 9, 'sept': 9, 'september': 9,
                'oct': 10, 'october': 10,
                'nov': 11, 'november': 11,
                'dec': 12, 'december': 12
            }
            
            month_num = month_map.get(month_str.lower())
            
            if filename == 'prorepma2013ext20130718.pdf':   # special treatment for typo
                month_num = 5
            
            if month_num:
                return datetime(int(year), month_num, 1)
    
    return None


def extract_tables_from_pdf(pdf_path):
    """
    Extract 
            Table 1 (Single Family Insured Mortgage Portfolio Change during Month),
            Table 3 (Title I Insured Mortgage Portfolio), and
            Table 4 (Single-Family Insured Mortgage Endorsement Characteristic Shares)
            from a single PDF.
    """

    try:
        # Extract all tables from the PDF
        # Try UTF-8 first, then fall back to cp1252 if that fails
        encodings = ['utf-8', 'cp1252', 'latin-1']
        tables = None
        
        for encoding in encodings:
            try:
                tables_stream = tabula.read_pdf(
                    pdf_path,
                    pages='all',
                    multiple_tables=True,
                    pandas_options={'header': None},
                    encoding=encoding,
                    silent=True,
                    stream=True
                )
                break  # Success! Stop trying other encodings
            except UnicodeDecodeError:
                continue  # Try next encoding
            except Exception as e:
                # Some other error, not encoding-related
                raise e
        
        if tables_stream is None:
            raise Exception("Could not read PDF with any encoding")

        for encoding in encodings:
            try:
                tables = tabula.read_pdf(
                    pdf_path,
                    pages='all',
                    multiple_tables=True,
                    pandas_options={'header': None},
                    encoding=encoding,
                    silent=True
                )
                break  # Success! Stop trying other encodings
            except UnicodeDecodeError:
                continue  # Try next encoding
            except Exception as e:
                # Some other error, not encoding-related
                raise e
        
        if tables is None:
            raise Exception("Could not read PDF with any encoding")
     
     
        # Identify tables by looking for identifying text
        table1_df = None
        table3_df = None
        table4_df = None
        
        
        for i, table in enumerate(tables):
            # Convert first few rows to string to search
            table_text = table.to_string().lower()

            # Look for Table 1 identifiers
            if 'refinance with fha' in table_text and 'delinquency' not in table_text:
                table1_df = table.copy()
             
            # Look for Table 3 identifiers
            if 'property improvement' in table_text:
                table3_df = table.copy()
                break

        # Stream works better for table 4                
        for i, table in enumerate(tables_stream):
            # Convert first few rows to string to search
            table_text = table.to_string().lower()

            # Look for Table 4 identifiers
            if 'first-time homebuyer' in table_text or 'first time homebuyer' in table_text:
                table4_df = table.copy()
                break
        
        if table1_df is None:
            print(f"  Warning: Table 1 not found in {os.path.basename(pdf_path)}")

        if table3_df is None:
            print(f"  Warning: Table 3 not found in {os.path.basename(pdf_path)}")

        if table4_df is None:
            print(f"  Warning: Table 4 not found in {os.path.basename(pdf_path)}")
        
        # Clean the dataframes
        if table1_df is not None:
            table1_df = table1_df.dropna(how='all')  # Remove empty rows
        if table3_df is not None:
            table3_df = table3_df.dropna(how='all')  # Remove empty rows
        if table4_df is not None:
            table4_df = table4_df.dropna(how='all')  # Remove empty rows
        
        # Extract date from filename
        fndate = extract_date_from_filename(os.path.basename(pdf_path))
        
        # Parse the table structure
        data_dict1 = {'date': fndate, 'filename': os.path.basename(pdf_path)}
        data_dict3 = data_dict1.copy()
        data_dict4 = data_dict1.copy()
        
        if table1_df is not None:
            data_dict1 = extract_table1_from_pdf(data_dict1, table1_df, pdf_path)
        else:
            data_dict1 = None
            
        if table3_df is not None:
            data_dict3 = extract_table3_from_pdf(data_dict3, table3_df, pdf_path)
        else:
            data_dict3 = None
            
        if table4_df is not None:
            data_dict4 = extract_table4_from_pdf(data_dict4, table4_df, pdf_path)
        else:
            data_dict4 = None

        return data_dict1, data_dict3, data_dict4
        
    except Exception as e:
        print(f"  Error processing {os.path.basename(pdf_path)}: {e}")
        return None, None, None



def get_values(row):
    values = []
    for x in row:
        if pd.isna(x):
            continue
        s = str(x).strip().replace(',', '').replace('$', '')
        # Handle accounting negatives like "(123)"
        if s.startswith('(') and s.endswith(')'):
            s = '-' + s[1:-1]
        if s == '-':
            s = '0'
        # Now check if it's a valid number
        try:
            float(s)
            values.append(s)
        except ValueError:
            continue
    
    # If there's only 1 value, we don't know if it's in column 1 or 2
    if len(values) < 2:
        values = [None, None]      
        print ("**** Missing Values in Table **** \nROW: ", row, "\nvalues: ", values, "s: ", s)
        
    return values

def extract_table1_from_pdf(data_dict, table_df, pdf_path):
    try:
        
        # Convert to string for easier parsing
        for idx, row in table_df.iterrows():
            row_text = ' '.join([str(x).lower() for x in row if pd.notna(x)])
            row_text = row_text.replace('-', ' ')
            if not any(ch.isdigit() for ch in row_text):
                continue
                        
            if len(row) == 1:
                row = row[0].split()
            
            # Extract key metrics
            if 'insurance in force (beginning)' in row_text:
                values = get_values(row)
                data_dict['insurance_beg_k'] = values[0]
                data_dict['insurance_beg_b'] = values[1]                           

            if 'prepayments' in row_text:
                values = get_values(row)
                data_dict['prepay_k'] = values[0]
                data_dict['prepay_b'] = values[1]                           

            if 'refinance with fha' in row_text:
                values = get_values(row)
                data_dict['refi_fha_k'] = values[0]
                data_dict['refi_fha_b'] = values[1]                           

            if 'full payoff' in row_text:
                values = get_values(row)
                data_dict['payoff_k'] = values[0]
                data_dict['payoff_b'] = values[1]                           

            if 'claims' in row_text:
                values = get_values(row)
                data_dict['claims_k'] = values[0]
                data_dict['claims_b'] = values[1]                           

            if 'conveyance' in row_text:
                values = get_values(row)
                data_dict['conveyance_k'] = values[0]
                data_dict['conveyance_b'] = values[1]                           

            if 'pre foreclosure sale' in row_text:
                values = get_values(row)
                data_dict['pre_foreclosure_sale_k'] = values[0]
                data_dict['pre_foreclosure_sale_b'] = values[1]                           

            if 'note sales' in row_text:
                values = get_values(row)
                data_dict['note_sale_k'] = values[0]
                data_dict['note_sale_b'] = values[1]                           

            if 'third party sales' in row_text:
                values = get_values(row)
                data_dict['third_party_sale_k'] = values[0]
                data_dict['third_party_sale_b'] = values[1]                           

            if 'endorsements' in row_text:
                values = get_values(row)
                data_dict['endorsements_k'] = values[0]
                data_dict['endorsemenst_b'] = values[1]                           

            if 'adjustment' in row_text:
                values = get_values(row)
                data_dict['adjustment_k'] = values[0]
                data_dict['adjustment_b'] = values[1]                           

            if 'insurance in force (ending)' in row_text:
                values = get_values(row)
                data_dict['insurance_end_k'] = values[0]
                data_dict['insurance_end_b'] = values[1]                           

        return data_dict

    except Exception as e:
        print(f"  Error processing Table 1 in {os.path.basename(pdf_path)}: {e}")
        print("\nraw: ", row_text)
        print("\nparsed: ", values)
        print("\nTable:\n", table_df)
        return None    
        
def extract_table3_from_pdf(data_dict, table3_df, pdf_path):

    try:
     
        section = ''
        
        # Convert to string for easier parsing
        for idx, row in table3_df.iterrows():
            row_text = ' '.join([str(x).lower() for x in row if pd.notna(x)])
            row_text = row_text.replace('-', ' ')  # this assumes negative numbers always use ()
            if not any(ch.isdigit() for ch in row_text):
                continue
            
            if len(row) == 1:
                row = row[0].split()
            
            # Extract key metrics
            if 'insurance in force (beginning)' in row_text:
                values = get_values(row)
                section = 'insurance_beg'
                data_dict[section+'_tot_k'] = values[0]
                data_dict[section+'_tot_b'] = values[1]                           

            if 'prepayments' in row_text:
                values = get_values(row)
                section = 'prepayment'
                data_dict[section+'_tot_k'] = values[0]
                data_dict[section+'_tot_b'] = values[1]                              
                
            if 'claims' in row_text:
                values = get_values(row)
                section = 'claims'
                data_dict[section+'_tot_k'] = values[0]
                data_dict[section+'_tot_b'] = values[1]    

            if 'endorsements' in row_text:
                values = get_values(row)
                section = 'endorsements'
                data_dict[section+'_tot_k'] = values[0]
                data_dict[section+'_tot_b'] = values[1]    

            if 'adjustment' in row_text:
                values = get_values(row)
                section = 'adjustment'
                data_dict[section+'_tot_k'] = values[0]
                data_dict[section+'_tot_b'] = values[1]    

            if 'insurance in force (ending)' in row_text:
                values = get_values(row)
                section = 'insurance_end'
                data_dict[section+'_tot_k'] = values[0]
                data_dict[section+'_tot_b'] = values[1]    
                                                    
            if 'property improvement' in row_text:
                values = get_values(row)
                data_dict[section+'_pi_k'] = values[0]
                data_dict[section+'_pi_b'] = values[1]                              

            if 'manufactured housing' in row_text:
                values = get_values(row)
                data_dict[section+'_mh_k'] = values[0]
                data_dict[section+'_mh_b'] = values[1]                              
 
        return data_dict
    
    except Exception as e:
        print(f"  Error processing Table 3 in {os.path.basename(pdf_path)}: {e}")
        print("\nraw: ", row_text)
        print("\nparsed: ", values)
        print("\nTable:\n", table3_df)
        return None    


def get_percentage(row):
    """Extract percentage value from row"""
    for x in row:
        if pd.isna(x):
            continue
        s = str(x).strip()
        # Look for percentage pattern
        if '%' in s or (s.replace('.', '').replace('-', '').isdigit() and '.' in s):
            s = s.replace('%', '').strip()
            try:
                return float(s)
            except ValueError:
                continue
    return None


def extract_table4_from_pdf(data_dict, table4_df, pdf_path):
    """
    Extract Table 4 (Single-Family Insured Mortgage Endorsement Characteristic Shares)
    """
    try:
        # We need to find the column with the current month data
        # This is typically the first numeric column after the row labels
        
        for idx, row in table4_df.iterrows():
            row_text = ' '.join([str(x).lower() for x in row if pd.notna(x)])
            # this assumes negative numbers always use ()
            row_text = row_text.replace('-', ' ').replace('–', ' ').replace('—', ' ').replace(r'\\r', ' ') 

            # Skip rows without relevant text
            if not any(ch.isalpha() for ch in row_text):
                continue

            # Sometimes Tabula doesn't split columns properly
            if len(row) < 3:
                row = row[0].split()
            
            # Total Endorsement Count
            if 'total endorsement count' in row_text:
                values = get_values(row)
                if values[0]:
                    data_dict['total_endorsement_count'] = values[0]
            
            # Purchase/Refinance shares
            if row_text.strip().startswith('purchase (%)') or row_text.strip() == 'purchase (%)':
                pct = get_percentage(row)
                if pct:
                    data_dict['purchase_pct'] = pct
                    
            if row_text.strip().startswith('refinance (%)') or row_text.strip() == 'refinance (%)':
                pct = get_percentage(row)
                if pct:
                    data_dict['refinance_pct'] = pct
            
            # Purchase Loan Count
            if 'purchase loan count' in row_text and 'shares' not in row_text:
                values = get_values(row)
                if values[0]:
                    data_dict['purchase_loan_count'] = values[0]
            
            # First-Time Homebuyer
            if 'first time homebuyer' in row_text or 'first-time homebuyer' in row_text:
                pct = get_percentage(row)
                if pct:
                    data_dict['first_time_homebuyer_pct'] = pct
            
            # 203(K)
            if '203(k)' in row_text.lower() or '203k' in row_text.lower():
                pct = get_percentage(row)
                if pct:
                    data_dict['203k_pct'] = pct
            
            # Minority percentages
            if row_text.strip().startswith('minority (%)') or row_text.strip() == 'minority (%)':
                pct = get_percentage(row)
                if pct:
                    data_dict['minority_pct'] = pct
                    
            if 'non minority (%)' in row_text or 'non-minority (%)' in row_text:
                pct = get_percentage(row)
                if pct:
                    data_dict['non_minority_pct'] = pct
                    
            if 'undisclosed race' in row_text or 'undisclosed race/ethnicity' in row_text:
                pct = get_percentage(row)
                if pct:
                    data_dict['undisclosed_race_pct'] = pct
            
            # Refinance Loan Count
            if 'refinance loan count' in row_text and 'shares' not in row_text:
                values = get_values(row)
                if values[0]:
                    data_dict['refinance_loan_count'] = values[0]
            
            # FHA Streamline
            if 'fha streamline' in row_text:
                pct = get_percentage(row)
                if pct:
                    data_dict['fha_streamline_pct'] = pct
            
            # Need to distinguish between the fha-to-fha and conventional-to-fha sections
            # FHA-to-FHA
            if 'fha to fha' in row_text or 'fha-to-fha' in row_text:
                if 'fully underwritten' in row_text:
                    section = 'fha_to_fha'
                    pct = get_percentage(row)
                    if pct:
                        data_dict['fha_to_fha_pct'] = pct
                        
             # Conventional-to-FHA
            if 'conventional to fha' in row_text or 'conventional-to-fha' in row_text:
                if 'non cash' not in row_text and 'cash out' not in row_text:
                    section = 'conv_to_fha'
                    pct = get_percentage(row)
                    if pct:
                        data_dict['conv_to_fha_pct'] = pct
                         
            # Non-cash-out (under FHA-to-FHA section)
            if 'non cash out' in row_text or 'non-cash-out' in row_text:
                # Need to track context - is this under FHA-to-FHA or Conventional-to-FHA?
                pct = get_percentage(row)
                if pct:
                    data_dict[section+'_noncash_pct'] = pct
            
            # Cash out
            if 'cash out' in row_text and 'non' not in row_text:
                pct = get_percentage(row)
                if pct:
                    data_dict[section+'_cashout_pct'] = pct

            
            # Property Type Shares
            if 'single family detached' in row_text or 'single-family detached' in row_text:
                pct = get_percentage(row)
                if pct:
                    data_dict['single_family_detached_pct'] = pct
                    
            if row_text.strip().startswith('townhome (%)') or row_text.strip() == 'townhome (%)':
                pct = get_percentage(row)
                if pct:
                    data_dict['townhome_pct'] = pct
                    
            if row_text.strip().startswith('condominium (%)') or row_text.strip() == 'condominium (%)':
                pct = get_percentage(row)
                if pct:
                    data_dict['condominium_pct'] = pct
                    
            if '2 4 unit' in row_text or '2-4 unit' in row_text:
                pct = get_percentage(row)
                if pct:
                    data_dict['2_4_unit_pct'] = pct
                    
            if 'manufactured housing' in row_text:
                pct = get_percentage(row)
                if pct:
                    data_dict['manufactured_housing_pct'] = pct
        
        return data_dict
    
    except Exception as e:
        print(f"  Error processing Table 4 in {os.path.basename(pdf_path)}: {e}")
        print("\nTable:\n", table4_df)
        print("\nrow_text: ", row_text)
        return None


def extract_tables_from_all_pdfs(out_path, pdf_path):
    """
    Extract Tables 1, 3, and 4 data from all PDFs in a directory and combine into DataFrames.
    """
    pdf_dir = Path(pdf_path)
    
    if not pdf_dir.exists():
        print(f"Error: Directory '{out_path}' not found.")
        return None, None, None
    
    # Get all PDF files
    pdf_files = sorted(pdf_dir.glob("*.pdf"))
    
    if not pdf_files:
        print(f"No PDF files found in '{out_path}'")
        return None, None, None
    
    print(f"Found {len(pdf_files)} PDF files. Extracting tables...")
    
    all_data1 = []
    all_data3 = []
    all_data4 = []
    
    i = 0
    for pdf_file in pdf_files:
        i = i + 1
        print(f"Processing: {i} {pdf_file.name}")
        data1, data3, data4 = extract_tables_from_pdf(str(pdf_file))
        
        if data1 and len(data1) > 2:  # More than just date and filename
            all_data1.append(data1)
            
        if data3 and len(data3) > 2:  # More than just date and filename
            all_data3.append(data3)
            
        if data4 and len(data4) > 2:  # More than just date and filename
            all_data4.append(data4)
      
   
    # Create DataFrames
    df1 = pd.DataFrame(all_data1) if all_data1 else pd.DataFrame()
    df3 = pd.DataFrame(all_data3) if all_data3 else pd.DataFrame()
    df4 = pd.DataFrame(all_data4) if all_data4 else pd.DataFrame()
    
    # Sort by date
    if not df1.empty and 'date' in df1.columns:
        df1 = df1.sort_values('date').reset_index(drop=True)

    if not df3.empty and 'date' in df3.columns:
        df3 = df3.sort_values('date').reset_index(drop=True)
        
    if not df4.empty and 'date' in df4.columns:
        df4 = df4.sort_values('date').reset_index(drop=True)
    
    print(f"\nSuccessfully extracted Table 1 from {len(df1)} reports.")
    print(f"Successfully extracted Table 3 from {len(df3)} reports.")
    print(f"Successfully extracted Table 4 from {len(df4)} reports.")
    
    if not df3.empty:
        print(f"Date range: {df3['date'].min()} to {df3['date'].max()}")
    
    return df1, df3, df4

def main(out_path, pdf_path, output_file = "fha_data"):
    """
    Main function to extract Tables 1, 3, and 4 data and save to CSV.
    """
    # Extract data

    os.makedirs(out_path, exist_ok=True)
    
    df1, df3, df4  = extract_tables_from_all_pdfs(out_path, pdf_path)
    
    if df1 is not None and not df1.empty:
        # Save to CSV
        df1.to_csv(out_path+output_file+"_tab1_"+date.today().isoformat()+".bak", index=False)  # archive
        df1.to_csv(out_path+output_file+"_tab1.csv", index=False)
        print(f"\nTable 1 Data saved to: {out_path+output_file+'_tab1.csv'}")

    if df3 is not None and not df3.empty:
        # Save to CSV
        df3.to_csv(out_path+output_file+"_tab3_"+date.today().isoformat()+".bak", index=False)  # archive
        df3.to_csv(out_path+output_file+"_tab3.csv", index=False)
        print(f"\nTable 3 Data saved to: {out_path+output_file+'_tab3.csv'}")

    if df4 is not None and not df4.empty:
        # Save to CSV
        df4.to_csv(out_path+output_file+"_tab4_"+date.today().isoformat()+".bak", index=False)  # archive
        df4.to_csv(out_path+output_file+"_tab4.csv", index=False)
        print(f"\nTable 4 Data saved to: {out_path+output_file+'_tab4.csv'}")
 
        
    # Display summary statistics
    print("\n" + "="*60)
    print("SUMMARY STATISTICS")
    print("="*60)
    print(f"\nTotal months extracted:")
    print(f"  Table 1: {len(df1)}")
    print(f"  Table 3: {len(df3)}")
    print(f"  Table 4: {len(df4)}")
    
    if not df1.empty:
        print(f"\nTable 1 Columns available: {', '.join(df1.columns)}")
    if not df3.empty:
        print(f"\nTable 3 Columns available: {', '.join(df3.columns)}")
    if not df4.empty:
        print(f"\nTable 4 Columns available: {', '.join(df4.columns)}")
         
    # Display first few rows
    if not df1.empty:
        print("\n" + "="*60)
        print("FIRST FEW ROWS of Table 1")
        print("="*60)
        print(df1.head())

    if not df3.empty:
        print("\n" + "="*60)
        print("FIRST FEW ROWS of Table 3")
        print("="*60)
        print(df3.head())

    if not df4.empty:
        print("\n" + "="*60)
        print("FIRST FEW ROWS of Table 4")
        print("="*60)
        print(df4.head())
        
    return df1, df3, df4

if __name__ == "__main__":
    # Install required package if not already installed:
    # pip install tabula-py
    # Note: Also requires Java to be installed on your system
    
    df1, df3, df4 = main(out_path="./output/", pdf_path="./pdf/", output_file="fha_data")
