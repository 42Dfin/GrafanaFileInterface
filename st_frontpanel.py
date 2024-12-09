# import streamlit as st
# from file_management import convert1, stitch_csv_files
# from add_to_db import upload_data_to_postgres

# # Configure database parameters
# db_params = {
#     'host': 'aws-0-ap-southeast-2.pooler.supabase.com',
#     'port': '6543',
#     'database': 'postgres',
#     'user': 'postgres.wmqpmfnepjmjqognnnpx',
#     'password': 'Jonathan_42',
#     'sslmode': 'disable'
# }

# # Initialize UI
# st.title("File Processing Application")

# # File upload widget
# uploaded_files = st.file_uploader("Upload files", accept_multiple_files=True)

# # Array to hold selected files for processing
# selected_files = []

# # File selection
# if uploaded_files:
#     for uploaded_file in uploaded_files:
#         with open(uploaded_file.name, 'wb') as f:
#             f.write(uploaded_file.read())
#         selected_files.append(uploaded_file.name)

#     st.write(f"Selected files: {selected_files}")

# # Extract logs button
# if st.button("Extract Logs"):
#     convert1()
#     st.success("Logs extracted successfully!")

# # Apply to DB button
# if st.button("Apply to DB"):
#     if selected_files:
#         stitched_file = 'out.csv'
#         stitch_csv_files(selected_files, stitched_file)
#         upload_data_to_postgres(stitched_file, db_params)
#         st.success("Data applied to the database successfully!")

import os
import streamlit as st
from file_management import convert1, stitch_csv_files
from add_to_db import upload_data_to_postgres

# Configure database parameters
db_params = {
    'host': 'aws-0-ap-southeast-2.pooler.supabase.com',
    'port': '6543',
    'database': 'postgres',
    'user': 'postgres.wmqpmfnepjmjqognnnpx',
    'password': 'Jonathan_42',
    'sslmode': 'disable'
}

# Initialize UI
st.title("File Processing Application")

# File upload widget
uploaded_files = st.file_uploader("Upload .tar.gz files", accept_multiple_files=True)

# Array to hold selected files for processing
selected_files = []

# Folder where the 'P' files are stored after extraction
p_data_folder = './P_data'

# File selection
if uploaded_files:
    # Save and upload the .tar.gz files
    for uploaded_file in uploaded_files:
        with open(uploaded_file.name, 'wb') as f:
            f.write(uploaded_file.read())
    
    # Call convert1() to handle extraction and moving of files
    convert1()  # Assumes this function will extract and move the files to `P_data`

    # List all the P files available for selection in the P_data folder
    p_files = [os.path.join(p_data_folder, f) for f in os.listdir(p_data_folder) if f.startswith('P') and not f.endswith('.tar.gz')]
    
    if p_files:
        st.write("Available 'P' files:")
        for idx, p_file in enumerate(p_files):
            st.write(f"{idx + 1}. {p_file}")
        
        # Allow user to select files to process
        selected_file_indexes = st.multiselect("Select files to process", options=range(1, len(p_files) + 1), format_func=lambda x: p_files[x - 1])

        # Add selected files to the list
        selected_files = [p_files[i - 1] for i in selected_file_indexes]
        st.write(f"Selected files: {selected_files}")

# # Extract logs button
# if st.button("Extract Logs"):
#     convert1()  # Call the convert1() function to extract logs (if needed again)
#     st.success("Logs extracted successfully!")

# Apply to DB button
if st.button("Apply to DB"):
    if selected_files:
        stitched_file = 'out.csv'
        stitch_csv_files(selected_files, stitched_file)
        upload_data_to_postgres(stitched_file, db_params)
        st.success("Data applied to the database successfully!")

