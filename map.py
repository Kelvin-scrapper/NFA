# map.py

"""
NFA PROCESSOR - VERSION 9.0 (Section-Context Mapping)
- Resolves duplicate fund names using parent-section context tracked as rows are
  scanned. SECTION_HEADERS in config.py defines which fund names mark a new section;
  SECTION_CONTEXT_KEYS maps (section, fund_name) to the correct config key.
- Order-independent: moving rows within a section does not break the mapping.
- "andre rentefond" market_share=1.0 self-total rows are skipped — they are exact
  duplicates of the cross-aggregate row and would cause double-counting.
- Correctly implements conditional number parsing for 'Total' rows.
- Works in tandem with config.py.
"""

import pandas as pd
import zipfile
from datetime import datetime
import os
import glob
import re
import csv
import io

from config import CODES_CSV_STRING, DESCRIPTIONS_CSV_STRING, FUND_MAPPINGS, SECTION_HEADERS, SECTION_CONTEXT_KEYS

class NfaProcessor:
    def __init__(self):
        print("Initializing NFA Processor v9.0 (Section-Context Mapping)...")
        self.format_codes, self.format_descriptions = [], []
        self.fund_mappings = FUND_MAPPINGS
        self.unmapped_funds = set()
        self._load_format_from_config()
        print("Processor initialized successfully.")

    def _load_format_from_config(self):
        try:
            codes_file = io.StringIO(CODES_CSV_STRING.strip())
            descriptions_file = io.StringIO(DESCRIPTIONS_CSV_STRING.strip())
            self.format_codes = next(csv.reader(codes_file))[1:]
            self.format_descriptions = next(csv.reader(descriptions_file))[1:]
            print(f"   - Loaded {len(self.format_codes)} column definitions from config.")
        except Exception as e:
            print(f"CRITICAL ERROR: Could not load format from config.py: {e}")
            raise

    def get_fund_codes(self, fund_name, file_type, current_section=None):
        """
        Resolves output codes using parent-section context.

        SECTION_CONTEXT_KEYS maps (current_section, fund_name) → config key for
        fund names that appear more than once. current_section is the most recent
        section header seen BEFORE this row, so context is the parent — not the row
        itself. This makes mapping order-independent: moving rows within a section
        does not break the lookup.
        """
        fund_key = " ".join(fund_name.strip().split()).lower()
        mapping_key = SECTION_CONTEXT_KEYS.get((current_section, fund_key), fund_key)

        if mapping_key in self.fund_mappings:
            codes = self.fund_mappings[mapping_key]
            if file_type == 'NORRETCUS':
                return codes.get('netsub_norretcus'), codes.get('mancap_norretcus')
            elif file_type == 'PENFUNDSEL':
                return codes.get('netsub_penfundsel'), codes.get('mancap_penfundsel')

        return None, None

    def _parse_number(self, value, is_total_row=False):
        """
        Conditionally parses a number string to a float based on the row type.
        - If is_total_row is True: Removes dots as thousands separators.
        - If is_total_row is False: Treats commas as thousands separators and dots as decimals.
        """
        if pd.isna(value):
            return 0.0
        
        num_str = str(value).strip()

        if is_total_row:
            # Rule for 'Total' rows: remove all dots, treat comma as decimal.
            num_str = num_str.replace('.', '').replace(',', '.')
        else:
            # Default rule for all other rows: treat comma as decimal separator.
            num_str = num_str.replace(',', '.')

        return pd.to_numeric(num_str, errors='coerce', downcast=None)

    def process_directory(self, scan_dir=".", output_dir="output"):
        print("\n" + "="*60 + "\nSTARTING FILE PROCESSING\n" + "="*60)
        all_records = []
        excel_files = self._scan_for_excel_files(scan_dir)

        for file_path in excel_files:
            file_format, sheet_name = self._sniff_file_format(file_path)
            if not file_format:
                print(f"\nSkipping file: {os.path.basename(file_path)} (Not a recognized NFA format)")
                continue

            print(f"\nProcessing file: {os.path.basename(file_path)} (Detected as {file_format})")
            records = []
            if file_format == "Tabell 2":
                records = self._process_detailed_file(file_path, sheet_name)
            elif file_format == "Tabell 1":
                records = self._process_summary_file(file_path, sheet_name)
            
            if records:
                all_records.extend(records)
        
        if not all_records:
            print("\nNo data could be extracted from any files.")
            return

        self._generate_final_report(all_records, output_dir)
        
    def _scan_for_excel_files(self, scan_directory):
        import hashlib
        print(f"Scanning for Excel files in: {os.path.abspath(scan_directory)}")
        excel_files = glob.glob(os.path.join(scan_directory, "**", "*.xls*"), recursive=True)
        candidates = [f for f in excel_files if not os.path.basename(f).startswith(('~$', '.'))]

        seen_hashes = {}
        unique = []
        for f in candidates:
            md5 = hashlib.md5(open(f, 'rb').read()).hexdigest()
            if md5 in seen_hashes:
                print(f"   - SKIPPING duplicate file: {os.path.basename(f)} (same content as {os.path.basename(seen_hashes[md5])})")
            else:
                seen_hashes[md5] = f
                unique.append(f)
        return unique

    def _sniff_file_format(self, file_path):
        try:
            xl = pd.ExcelFile(file_path, engine='openpyxl')
            if "Tabell 2" in xl.sheet_names: return "Tabell 2", "Tabell 2"
            if "Tabell 1" in xl.sheet_names: return "Tabell 1", "Tabell 1"
            return None, None
        except Exception:
            return None, None

    def _process_detailed_file(self, file_path, sheet_name):
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, engine='openpyxl')
            _, time_period, customer_type = self._get_file_metadata(filename=os.path.basename(file_path))
            
            records = []
            current_section = None
            start_row = 0
            for i, row in df.iterrows():
                if 'navn' in str(row.iloc[0]).lower():
                    start_row = i + 1; break

            for i in range(start_row, len(df)):
                if pd.isna(df.iloc[i, 0]) or len(str(df.iloc[i, 0]).strip()) == 0 or len(df.columns) <= 5: continue

                fund_name = str(df.iloc[i, 0])
                fund_key = " ".join(fund_name.strip().split()).lower()
                is_total = fund_key == 'total'

                # Skip "andre rentefond" self-total (market_share == 1.0).
                # This row has the identical value to the cross-aggregate row above it
                # and maps to the same output code — processing it causes double-counting.
                try:
                    ms = float(df.iloc[i, 8]) if len(df.columns) > 8 else None
                    if ms is not None and abs(ms - 1.0) < 0.001 and fund_key == "andre rentefond":
                        continue
                except (TypeError, ValueError):
                    pass

                netsub_val = self._parse_number(df.iloc[i, 4], is_total_row=is_total)
                mancap_val = self._parse_number(df.iloc[i, 5], is_total_row=is_total)

                # Resolve codes using current_section BEFORE updating it for this row.
                # This ensures the section reflects the parent context, not the row itself.
                netsub_code, mancap_code = self.get_fund_codes(fund_name, customer_type, current_section)

                # Update section AFTER code lookup so subsequent rows see this as parent.
                if fund_key in SECTION_HEADERS:
                    current_section = fund_key

                if abs(netsub_val) < 0.01 and abs(mancap_val) < 0.01:
                    continue

                if netsub_code and mancap_code:
                    records.extend([
                        {'code': netsub_code, 'value': netsub_val, 'period': time_period},
                        {'code': mancap_code, 'value': mancap_val, 'period': time_period}
                    ])
                else:
                    self.unmapped_funds.add(fund_name.strip())
            
            print(f"   - Extracted {len(records)} detailed data points.")
            return records
        except Exception as e:
            print(f"   - ERROR during detailed file processing: {e}")
            return []

    def _process_summary_file(self, file_path, sheet_name):
        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, header=None, engine='openpyxl')
            _, time_period, customer_type = self._get_file_metadata(filename=os.path.basename(file_path))
            
            total_row = df[df[0].astype(str).str.lower() == 'total']
            if total_row.empty: return []

            # In this file, we are ONLY processing the 'Total' row, so is_total_row is always True
            netsub_val = self._parse_number(total_row.iloc[0, 4], is_total_row=True)
            mancap_val = self._parse_number(total_row.iloc[0, 5], is_total_row=True)

            netsub_code, mancap_code = self.get_fund_codes('Total', customer_type)
            if netsub_code and mancap_code:
                print(f"   - Extracted 2 summary 'Total' data points.")
                return [
                    {'code': netsub_code, 'value': netsub_val, 'period': time_period},
                    {'code': mancap_code, 'value': mancap_val, 'period': time_period}
                ]
            return []
        except Exception as e:
            print(f"   - ERROR during summary file processing: {e}")
            return []
            
    def _get_file_metadata(self, filename):
        time_period = "2025-06"
        month_patterns = {'januar': '01', 'februar': '02', 'mars': '03', 'april': '04', 'mai': '05', 'juni': '06', 'juli': '07', 'august': '08', 'september': '09', 'oktober': '10', 'november': '11', 'desember': '12'}
        for name, num in month_patterns.items():
            if name in filename.lower():
                year = re.search(r'20\d{2}', filename.lower())
                if year: time_period = f"{year.group()}-{num}"; break
        
        customer_type = 'NORRETCUS'
        if 'pensjon' in filename.lower(): customer_type = 'PENFUNDSEL'
        
        return filename, time_period, customer_type

    def _generate_final_report(self, all_records, output_dir):
        print("\n" + "="*60 + "\nPROCESSING SUMMARY\n" + "="*60)
        print(f"   - Total data points extracted: {len(all_records)}")
        
        unmapped_list = sorted([f for f in self.unmapped_funds if f.lower() not in ['total', 'navn']])
        if unmapped_list:
            print("   - WARNING: The following fund types were found but NOT MAPPED:")
            for fund in unmapped_list: print(f"     - '{fund}'")
            print("   - ACTION: Add these to the FUND_MAPPINGS dictionary in config.py.")
        else:
            print("   - All found fund types were successfully mapped.")

        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        df_data = pd.DataFrame(all_records).pivot_table(index='period', columns='code', values='value', aggfunc='sum', fill_value=0)
        df_data = df_data.reindex(columns=self.format_codes, fill_value=0)
        
        data_path = os.path.join(output_dir, f"NFA_DATA_{timestamp}.xlsx")
        meta_path = os.path.join(output_dir, f"NFA_META_{timestamp}.xlsx")
        zip_path = os.path.join(output_dir, f"NFA_{timestamp}.ZIP")

        with pd.ExcelWriter(data_path, engine='openpyxl') as writer:
            header_df = pd.DataFrame([self.format_codes, self.format_descriptions])
            header_df.to_excel(writer, sheet_name='Data', index=False, header=False, startrow=0, startcol=1)
            df_data.to_excel(writer, sheet_name='Data', index=True, header=False, startrow=2)
        print(f"\nData file created: {os.path.basename(data_path)}")

        df_meta = pd.DataFrame({'CODE': self.format_codes, 'DESCRIPTION': self.format_descriptions, 'UNIT': 'I tusen NOK', 'FREQUENCY': 'M', 'SOURCE': 'NFAMA', 'DATASET': 'NFA', 'NEXT_RELEASE_DATE': (datetime.now() + pd.DateOffset(months=1)).strftime('%Y-%m-01T12:00:00')})
        df_meta.to_excel(meta_path, sheet_name='Metadata', index=False)
        print(f"Metadata file created: {os.path.basename(meta_path)}")

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.write(data_path, os.path.basename(data_path)); zf.write(meta_path, os.path.basename(meta_path))
        print(f"ZIP archive created: {os.path.basename(zip_path)}")

        print("\n" + "="*60 + "\nPROCESSING COMPLETED\n" + "="*60)
        print(f"Final ZIP archive is ready at: {zip_path}")

if __name__ == "__main__":
    try:
        NfaProcessor().process_directory()
    except Exception as e:
        print(f"\nA critical error stopped the script: {e}")