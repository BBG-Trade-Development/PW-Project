from flask import Flask, request, render_template, send_file, flash, redirect, url_for, jsonify
import pandas as pd
import re
from datetime import datetime
import os
import sys
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment, Border, Side, Font, PatternFill

def resource_path(relative_path):
    """Get absolute path to resource, works for dev and for PyInstaller"""
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except AttributeError:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

def get_output_dir():
    """Get stable output directory alongside the executable or script."""
    if getattr(sys, 'frozen', False):
        base_path = os.path.dirname(sys.executable)
    else:
        base_path = os.path.dirname(os.path.abspath(__file__))
    out_dir = os.path.join(base_path, 'outputs')
    os.makedirs(out_dir, exist_ok=True)
    return out_dir

# Initialize Flask app with proper template folder
app = Flask(__name__, template_folder=resource_path('templates'))
app.secret_key = 'your-secret-key-change-this-in-production'

# # Create outputs directory
# output_dir = resource_path('outputs')
# os.makedirs(output_dir, exist_ok=True)

# File paths

# Initialize output_dir once
output_dir = get_output_dir()

price_book_path = resource_path('Price_Book_Full.xlsx')
zpurcon_path = resource_path('ZPURCON.xlsx')

def process_data(vendor_id, gp2_threshold):
    try:
        # Load Excel files
        print(f"Loading Price Book from: {price_book_path}")
        print(f"Loading ZPURCON from: {zpurcon_path}")
        
        if not os.path.exists(price_book_path):
            return None, f"Price Book file not found at: {price_book_path}"
        if not os.path.exists(zpurcon_path):
            return None, f"ZPURCON file not found at: {zpurcon_path}"
            
        PB = pd.read_excel(price_book_path, sheet_name='Printer Friendly')
        ZPUR = pd.read_excel(zpurcon_path)
        
        print(f"✅ Loaded Price Book with {len(PB)} rows")
        print(f"✅ Loaded ZPURCON with {len(ZPUR)} rows")
        
    except FileNotFoundError as e:
        return None, f"Required Excel file not found: {e}"
    except Exception as e:
        return None, f"Error loading Excel files: {e}"

    cols_to_merge = [
        'Supplier', 'Price Group #', 'Price Group Description', 'FOB', 'SPA', 'Miscellaneous',
        'Land Freight', 'Ocean Freight', 'Federal Tax', 'Broker Charge', 'Bulk Whiskey Fee',
        'Duty', 'Tariffs Per Case', 'Consolidate Fee', 'Gallonage tax per case pd to Vendor',
        'Gallonage tax per case Pd to State', 'Gallonage tax Volume based Pd to State',
        'Total', 'Mov Avg 7210', 'Stock in bottles', 'Stock in Cases', 'Mrp Controller'
    ]

    # Filter cols_to_merge to only include columns that actually exist in ZPUR
    existing_cols_to_merge = [col for col in cols_to_merge if col in ZPUR.columns]
    
    if not existing_cols_to_merge:
        return None, "No matching columns found for merging data."

    print(f"Merging columns: {existing_cols_to_merge}")

    # Merge dataframes
    PB_merged = PB.merge(
        ZPUR[['Material'] + existing_cols_to_merge],
        left_on='SAP Product ID',
        right_on='Material',
        how='left'
    ).drop(columns=['Material'])

    # Filter by Vendor ID with numeric coercion
    PB_merged['Supplier_str'] = pd.to_numeric(PB_merged['Supplier'], errors='coerce').fillna(0).astype(int).astype(str).str.zfill(6)
    PB_input = PB_merged[PB_merged['Supplier_str'] == vendor_id].copy()
    PB_input.drop(columns=['Supplier_str'], inplace=True)

    if PB_input.empty:
        return None, f"Vendor ID {vendor_id} was not found in the data."

    print(f"✅ Found {len(PB_input)} records for vendor {vendor_id}")

    # Deduplicate (BLOCK 4) - only if all subset columns exist
    dedup_cols = [
        'Pricing Type', 'Deal ID', 'Deal Class', 'Channel',
        'Purchase Quantity', 'Price Group', 'Brand', 'Vendor ID'
    ]
    existing_dedup_cols = [c for c in dedup_cols if c in PB_input.columns]
    
    if len(existing_dedup_cols) >= 4:  # Only dedupe if we have most key columns
        PW_deduped = PB_input.drop_duplicates(subset=existing_dedup_cols)
        print(f"✅ Deduplication applied using columns: {existing_dedup_cols}")
        print(f"✅ After deduplication: {len(PW_deduped)} records")
    else:
        print(f"⚠️ Skipping deduplication - insufficient key columns available")
        PW_deduped = PB_input.copy()

    # Parse date columns safely if present
    for date_col in ['Start Date', 'End Date']:
        if date_col in PW_deduped.columns:
            PW_deduped[date_col] = pd.to_datetime(PW_deduped[date_col], errors='coerce')

    # Prepare for Excel writing
    # Define formats and styles
    accounting_format = '_($* #,##0.00_);_($* (#,##0.00);_($* "-"??_);_(@_)'
    percentage_format = '0.00%'
    light_blue_fill = PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
    light_red_fill = PatternFill(start_color="FFCCCC", end_color="FFCCCC", fill_type="solid")

    # Currency and percentage columns for formatting - check existence
    pw_currency_cols = [
        'List Case', 'List Bottle', 'Discount', 'Case Price', 'Bottle Price', 'Chargeback',
        'Replacement Cost', 'Floor Stock Value'
    ]
    pw_percentage_cols = [
        'GP2 - Replacement Cost', 'GP2 - Floor Stock Value'
    ]

    # Prepare output filename and path
    vendor_name_raw = "UnknownVendor"
    if 'Vendor' in PW_deduped.columns:
        unique_vendor_names = PW_deduped['Vendor'].dropna().unique()
        if len(unique_vendor_names) > 0:
            vendor_name_raw = str(unique_vendor_names[0])
    
    vendor_name_sanitized = re.sub(r'[^\w-]', '', vendor_name_raw).replace(' ', '_').strip('_')
    if not vendor_name_sanitized:
        vendor_name_sanitized = "NoName"

    now = datetime.now()
    datetime_str = now.strftime("%Y%m%d_%H%M%S")
    filename = f"PW_{vendor_id}_{vendor_name_sanitized}_{datetime_str}.xlsx"
    output_path = os.path.join(output_dir, filename)



# Cleaning numeric inputs

# Preventing divide-by-zero errors

# Deriving bottle-level and margin-level values

# Preparing this cleaned DataFrame to be written to Excel

    try:
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            # === GP2 Below Threshold Tab ===
            print("🔄 Generating 'GP2 Below Threshold' tab...")
            gp2_filtered_df = PW_deduped.copy()

            # Columns for numeric conversion - skip if missing
            gp2_numeric_cols = ['Units Per Case', 'Case Price', 'Replacement Cost', 'Chargeback', 'List Case']
            for col in gp2_numeric_cols:
                if col in gp2_filtered_df.columns:
                    gp2_filtered_df[col] = pd.to_numeric(gp2_filtered_df[col], errors='coerce').fillna(0)

            # Calculate derived columns safely
            if 'Units Per Case' in gp2_filtered_df.columns and 'List Case' in gp2_filtered_df.columns:
                gp2_filtered_df['List Bottle'] = gp2_filtered_df.apply(
                    lambda row: row['List Case'] / row['Units Per Case'] if row['Units Per Case'] != 0 else pd.NA,
                    axis=1
                )

            if 'Units Per Case' in gp2_filtered_df.columns and 'Case Price' in gp2_filtered_df.columns:
                gp2_filtered_df['Bottle Price'] = gp2_filtered_df.apply(
                    lambda row: row['Case Price'] / row['Units Per Case'] if row['Units Per Case'] != 0 else pd.NA,
                    axis=1
                )

            def safe_gp2_replacement_cost(row):
                try:
                    if row.get('Case Price', 0) != 0:
                        return (row['Case Price'] - (row.get('Replacement Cost', 0) - row.get('Chargeback', 0))) / row['Case Price']
                    else:
                        return pd.NA
                except Exception:
                    return pd.NA

            def safe_gp2_floor_stock_value(row):
                try:
                    if row.get('Case Price', 0) != 0:
                        return (row['Case Price'] - (row.get('Floor Stock Value', 0) - row.get('Chargeback', 0))) / row['Case Price']
                    else:
                        return pd.NA
                except Exception:
                    return pd.NA



# Calculate two GP2 margin values (Replacement Cost and Floor Stock Value),

# Filter the data to only include rows where these margins fall below a certain threshold,

# Then write that filtered data to an Excel worksheet, formatting the sheet nicely.

            gp2_filtered_df['GP2 - Replacement Cost'] = gp2_filtered_df.apply(safe_gp2_replacement_cost, axis=1)
            gp2_filtered_df['GP2 - Floor Stock Value'] = gp2_filtered_df.apply(safe_gp2_floor_stock_value, axis=1)

            # Filter by threshold
            gp2_filtered_df = gp2_filtered_df[
                (
                    (gp2_filtered_df['GP2 - Replacement Cost'].notna()) & 
                    (gp2_filtered_df['GP2 - Replacement Cost'] < gp2_threshold)
                ) | (
                    (gp2_filtered_df['GP2 - Floor Stock Value'].notna()) & 
                    (gp2_filtered_df['GP2 - Floor Stock Value'] < gp2_threshold)
                )
            ].copy()

            # Select columns to display
            gp2_cols_to_display = [
                'Price Group', 'Price Group Description', 'Pricing Type', 'Deal ID',
                'Deal Class', 'Purchase Quantity', 'Deal Description', 'Start Date',
                'End Date', 'List Case', 'Discount', 'Chargeback', 'Replacement Cost',
                'Floor Stock Value', 'List Bottle', 'Case Price', 'Bottle Price',
                'GP2 - Replacement Cost', 'GP2 - Floor Stock Value'
            ]
            gp2_cols_to_display = [col for col in gp2_cols_to_display if col in gp2_filtered_df.columns]
            gp2_output_df = gp2_filtered_df[gp2_cols_to_display].copy()

            if not gp2_output_df.empty:
                gp2_output_df.to_excel(writer, index=False, sheet_name='GP2 Below Threshold')
                worksheet_gp2 = writer.sheets['GP2 Below Threshold']
                worksheet_gp2.sheet_properties.tabColor = "FF9999"
                
                # Format headers
                for col_idx in range(1, worksheet_gp2.max_column + 1):
                    cell = worksheet_gp2.cell(row=1, column=col_idx)
                    cell.fill = light_blue_fill
                    cell.font = Font(bold=True)
                    cell.alignment = Alignment(wrap_text=True, horizontal='center', vertical='center')

                # Set column widths
                for column_cells in worksheet_gp2.columns:
                    length = max(len(str(cell.value)) if cell.value is not None else 0 for cell in column_cells)
                    col_letter = get_column_letter(column_cells[0].column)
                    worksheet_gp2.column_dimensions[col_letter].width = max(length + 4, 15)

                worksheet_gp2.freeze_panes = 'A2'

                # Apply formatting
                header_map_gp2 = {cell.value: cell.column for cell in worksheet_gp2[1]}
                
                # Currency formatting
                for col_name in pw_currency_cols:
                    if col_name in header_map_gp2:
                        col_idx = header_map_gp2[col_name]
                        for row_idx in range(2, worksheet_gp2.max_row + 1):
                            cell = worksheet_gp2.cell(row=row_idx, column=col_idx)
                            if isinstance(cell.value, (int, float)) and not pd.isna(cell.value):
                                cell.number_format = accounting_format

                # Percentage formatting
                for col_name in pw_percentage_cols:
                    if col_name in header_map_gp2:
                        col_idx = header_map_gp2[col_name]
                        for row_idx in range(2, worksheet_gp2.max_row + 1):
                            cell = worksheet_gp2.cell(row=row_idx, column=col_idx)
                            if isinstance(cell.value, (int, float)) and not pd.isna(cell.value):
                                cell.number_format = percentage_format
                                if cell.value < gp2_threshold:
                                    cell.fill = light_red_fill

                # Date formatting
                for date_col in ['Start Date', 'End Date']:
                    if date_col in header_map_gp2:
                        col_idx = header_map_gp2[date_col]
                        for row_idx in range(2, worksheet_gp2.max_row + 1):
                            cell = worksheet_gp2.cell(row=row_idx, column=col_idx)
                            if isinstance(cell.value, (datetime, pd.Timestamp)) and not pd.isna(cell.value):
                                cell.number_format = 'MM/DD/YYYY'

                print(f"✅ 'GP2 Below Threshold' tab written with {len(gp2_output_df)} rows.")
            else:
                print("⚠️ No records found below GP2 threshold")

            # === Brand tabs ===


#             The code generates one Excel worksheet per unique Brand in your data.

# Each sheet contains detailed pricing, deal, and margin info.

# It calculates derived columns and margin percentages for each brand separately.

# It dynamically formats sheets with styled headers, column widths, number formats, and conditional fills.

# You get well-organized, brand-specific Excel tabs ready for review or sharing.
            if 'Brand' in PW_deduped.columns:
                unique_brands = PW_deduped['Brand'].dropna().unique()
                print(f"Creating tabs for {len(unique_brands)} brands: {list(unique_brands)}")
                
                for brand in unique_brands:
                    brand_df = PW_deduped[PW_deduped['Brand'] == brand].copy()
                    
                    # Process brand data similar to GP2 processing
                    numeric_cols = ['Units Per Case', 'Case Price', 'Replacement Cost', 'Chargeback', 'List Case']
                    for col in numeric_cols:
                        if col in brand_df.columns:
                            brand_df[col] = pd.to_numeric(brand_df[col], errors='coerce').fillna(0)

                    # Calculate derived columns
                    if 'Units Per Case' in brand_df.columns and 'List Case' in brand_df.columns:
                        brand_df['List Bottle'] = brand_df.apply(
                            lambda row: row['List Case'] / row['Units Per Case'] if row['Units Per Case'] != 0 else pd.NA,
                            axis=1
                        )

                    if 'Units Per Case' in brand_df.columns and 'Case Price' in brand_df.columns:
                        brand_df['Bottle Price'] = brand_df.apply(
                            lambda row: row['Case Price'] / row['Units Per Case'] if row['Units Per Case'] != 0 else pd.NA,
                            axis=1
                        )

                    brand_df['GP2 - Replacement Cost'] = brand_df.apply(safe_gp2_replacement_cost, axis=1)
                    brand_df['GP2 - Floor Stock Value'] = brand_df.apply(safe_gp2_floor_stock_value, axis=1)

                    # Create pivot key if possible
                    pivot_key_cols = ['Channel', 'Pricing Type', 'Deal Class', 'Purchase Quantity']
                    existing_pivot_cols = [c for c in pivot_key_cols if c in brand_df.columns]
                    
                    if len(existing_pivot_cols) >= 2:
                        brand_df['Pivot Key'] = brand_df[existing_pivot_cols].astype(str).agg('_'.join, axis=1)
                    else:
                        brand_df['Pivot Key'] = 'N/A'

                    # Select columns for brand tab
                    brand_cols = [
                        'Pivot Key', 'Pricing Type', 'Deal ID', 'Deal Class', 'Channel',
                        'Purchase Quantity', 'Price Group', 'Price Group Description', 'Brand',
                        'Vendor ID', 'Vendor', 'Supplier', 'Case Price', 'Bottle Price',
                        'Replacement Cost', 'Chargeback', 'List Case', 'List Bottle',
                        'GP2 - Replacement Cost', 'GP2 - Floor Stock Value',
                        'Start Date', 'End Date', 'Deal Description'
                    ]
                    brand_cols = [col for col in brand_cols if col in brand_df.columns]
                    brand_tab_df = brand_df[brand_cols].copy()

                    # Create sheet name (max 31 chars for Excel)
                    sheet_name = str(brand)[:31]
                    brand_tab_df.to_excel(writer, index=False, sheet_name=sheet_name)
                    worksheet_brand = writer.sheets[sheet_name]

                    # Format brand sheet similar to GP2 sheet
                    for col_idx in range(1, worksheet_brand.max_column + 1):
                        cell = worksheet_brand.cell(row=1, column=col_idx)
                        cell.fill = light_blue_fill
                        cell.font = Font(bold=True)
                        cell.alignment = Alignment(wrap_text=True, horizontal='center', vertical='center')

                    for column_cells in worksheet_brand.columns:
                        length = max(len(str(cell.value)) if cell.value is not None else 0 for cell in column_cells)
                        col_letter = get_column_letter(column_cells[0].column)
                        worksheet_brand.column_dimensions[col_letter].width = max(length + 4, 15)

                    worksheet_brand.freeze_panes = 'A2'

                    # Apply formatting to brand sheet
                    header_map_brand = {cell.value: cell.column for cell in worksheet_brand[1]}
                    
                    for col_name in pw_currency_cols:
                        if col_name in header_map_brand:
                            col_idx = header_map_brand[col_name]
                            for row_idx in range(2, worksheet_brand.max_row + 1):
                                cell = worksheet_brand.cell(row=row_idx, column=col_idx)
                                if isinstance(cell.value, (int, float)) and not pd.isna(cell.value):
                                    cell.number_format = accounting_format

                    for col_name in pw_percentage_cols:
                        if col_name in header_map_brand:
                            col_idx = header_map_brand[col_name]
                            for row_idx in range(2, worksheet_brand.max_row + 1):
                                cell = worksheet_brand.cell(row=row_idx, column=col_idx)
                                if isinstance(cell.value, (int, float)) and not pd.isna(cell.value):
                                    cell.number_format = percentage_format
                                    if cell.value < gp2_threshold:
                                        cell.fill = light_red_fill

                    for date_col in ['Start Date', 'End Date']:
                        if date_col in header_map_brand:
                            col_idx = header_map_brand[date_col]
                            for row_idx in range(2, worksheet_brand.max_row + 1):
                                cell = worksheet_brand.cell(row=row_idx, column=col_idx)
                                if isinstance(cell.value, (datetime, pd.Timestamp)) and not pd.isna(cell.value):
                                    cell.number_format = 'MM/DD/YYYY'

                    print(f"✅ Created brand tab '{sheet_name}' with {len(brand_tab_df)} rows")

            # === Write main input tab ===
            PW_deduped.to_excel(writer, index=False, sheet_name='PW Input')
            worksheet_input = writer.sheets['PW Input']

            # Format input sheet
            for col_idx in range(1, worksheet_input.max_column + 1):
                cell = worksheet_input.cell(row=1, column=col_idx)
                cell.fill = light_blue_fill
                cell.font = Font(bold=True)

            for column_cells in worksheet_input.columns:
                length = max(len(str(cell.value)) if cell.value is not None else 0 for cell in column_cells)
                col_letter = get_column_letter(column_cells[0].column)
                worksheet_input.column_dimensions[col_letter].width = length + 4

            worksheet_input.freeze_panes = 'A2'

        print(f"✅ File saved: {output_path}")
        return filename, None

    except Exception as e:
        print(f"❌ Error creating Excel file: {str(e)}")
        return None, f"Error creating Excel file: {str(e)}"

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        vendor_id = request.form.get('vendor_id', '').strip()
        gp2_threshold = request.form.get('gp2_threshold', '').strip()

        # Validate inputs
        if not (vendor_id.isdigit() and len(vendor_id) == 6 and vendor_id.startswith('3')):
            flash("Vendor ID must be 6 digits and start with '3'.", 'error')
            return redirect(url_for('index'))

        try:
            gp2_threshold_val = float(gp2_threshold)
            if not (0 <= gp2_threshold_val <= 1):
                raise ValueError()
        except ValueError:
            flash("GP2 margin threshold must be a decimal between 0.00 and 1.00.", 'error')
            return redirect(url_for('index'))

        print(f"Processing vendor {vendor_id} with GP2 threshold {gp2_threshold_val}")
        filename, error = process_data(vendor_id, gp2_threshold_val)
        
        if error:
            flash(error, 'error')
            return redirect(url_for('index'))

        flash(f"Processing complete! Download your file: {filename}", 'success')
        return redirect(url_for('download_file', filename=filename))

    return render_template('index.html')

# ADD THIS NEW ROUTE FOR JSON API
@app.route('/api/process', methods=['POST'])
def api_process():
    try:
        # Get JSON data from request
        data = request.get_json()
        
        if not data:
            return jsonify({
                'success': False,
                'message': 'No JSON data received'
            }), 400
        
        vendor_id = str(data.get('vendor_id', '')).strip()
        gp2_threshold = str(data.get('gp2_threshold', '')).strip()

        
        # Validate inputs
        if not (vendor_id.isdigit() and len(vendor_id) == 6 and vendor_id.startswith('3')):
            return jsonify({
                'success': False,
                'message': "Vendor ID must be 6 digits and start with '3'."
            }), 400

        try:
            gp2_threshold_val = float(gp2_threshold)
            if not (0 <= gp2_threshold_val <= 1):
                raise ValueError()
        except ValueError:
            return jsonify({
                'success': False,
                'message': "GP2 margin threshold must be a decimal between 0.00 and 1.00."
            }), 400

        print(f"API Processing vendor {vendor_id} with GP2 threshold {gp2_threshold_val}")
        filename, error = process_data(vendor_id, gp2_threshold_val)
        
        if error:
            return jsonify({
                'success': False,
                'message': error
            }), 500

        # Return success with download URL
        download_url = url_for('download_file', filename=filename, _external=True)
        
        return jsonify({
            'success': True,
            'message': f'Processing complete! File generated: {filename}',
            'download_url': download_url
        })
        
    except Exception as e:
        print(f"API Error: {str(e)}")
        return jsonify({
            'success': False,
            'message': f'Server error: {str(e)}'
        }), 500

@app.route('/download/<filename>')
def download_file(filename):
    path = os.path.join(output_dir, filename)
    if os.path.exists(path):
        return send_file(path, as_attachment=True)
    else:
        flash("File not found.", 'error')
        return redirect(url_for('index'))

def open_browser():
    import webbrowser
    webbrowser.open_new('http://localhost:5000/')

if __name__ == '__main__':
    print("Starting Flask application...")
    print(f"Template folder: {app.template_folder}")
    print(f"Static folder: {app.static_folder}")
    print(f"Output directory: {output_dir}")
    
    # Check if required files exist
    if not os.path.exists(app.template_folder):
        print(f"❌ Templates folder not found: {app.template_folder}")
    else:
        print(f"✅ Templates folder found: {app.template_folder}")
    
    if not os.path.exists(price_book_path):
        print(f"❌ Price Book not found: {price_book_path}")
    else:
        print(f"✅ Price Book found: {price_book_path}")
        
    if not os.path.exists(zpurcon_path):
        print(f"❌ ZPURCON not found: {zpurcon_path}")
    else:
        print(f"✅ ZPURCON found: {zpurcon_path}")
    
    import threading
    threading.Timer(1.0, open_browser).start()
    app.run(debug=False, host='127.0.0.1', port=5000)
