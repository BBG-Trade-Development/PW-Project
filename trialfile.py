import os
import glob

# Get current user's profile path
user_profile = os.environ['USERPROFILE']

# Find the OneDrive - <OrgName> folder dynamically (may vary by user/org)
one_drive_base_pattern = os.path.join(user_profile, 'OneDrive - *')
one_drive_folders = glob.glob(one_drive_base_pattern)

if not one_drive_folders:
    raise FileNotFoundError("OneDrive folder not found. Please ensure SharePoint folder is synced via OneDrive.")

one_drive_path = one_drive_folders[0]  # Take the first match

# Build the local path to the synced SharePoint folder
# Note: The URL path parts become folder names with spaces decoded
local_path_parts = [
    'TDAnalysts-BBGCA',
    'Shared Documents',
    'PW Project',
    'Price_Book_Full.xlsx'
]

local_file_path = os.path.join(one_drive_path, *local_path_parts)

# Now you can load the Excel file with pandas
import pandas as pd

df = pd.read_excel(local_file_path)

print("Loaded data shape:", df.shape)
print(local_file_path)  # Shows normal path with single backslashes

