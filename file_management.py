import os
import tarfile
import shutil
import csv
import pandas as pd

#########EXTRACT TARS###############
def extract_tar(tar_path, extract_path):
    with tarfile.open(tar_path) as tar:
        tar.extractall(path=extract_path)

def recursively_extract_tars(root_dir):
    for root, dirs, files in os.walk(root_dir):
        for file in files:
            if file.endswith(".tar.gz"):
                file_path = os.path.join(root, file)
                extract_path = os.path.join(root, file[:-7])  # Remove .tar extension for the folder name
                os.makedirs(extract_path, exist_ok=True)
                extract_tar(file_path, extract_path)
                recursively_extract_tars(extract_path)  # Recursively extract any .tar files in the new directory


def move_files(source_dir, dest_dir):

    for root, dirs, files in os.walk(source_dir):
        for file in files:
            if file.startswith('P'):
                source_file_location = os.path.join(root, file)
                dest_file_location = os.path.join(dest_dir, file)

                shutil.move(source_file_location, dest_file_location)
                print(f"moved: {source_file_location} to {dest_file_location}")

def extract_and_move():
    source_dir = os.getcwd()
    dest_dir = os.path.join(source_dir, 'P_data')

    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    recursively_extract_tars(source_dir)
    move_files(source_dir, dest_dir)
    return dest_dir

######END EXTRACT TARS#####################4


##########CONVERT TXT TO CSV#################

def convert_txt_to_csv(input_file):
    output_file = input_file.replace('.txt', '.csv')

    # Open the input file and read all lines
    with open(input_file, 'r') as file:
        lines = file.readlines()
        
        # Extract the date from the third line (metadata)
        date_line = lines[2].strip()  # Third line should have the date
        date_parts = date_line.split()  # Split the line by spaces
        date = f"{date_parts[1]}-{date_parts[2]}-{date_parts[3]}"  # Format as YYYY-MM-DD

        # Remove metadata lines to keep only data
        lines = lines[4:]  # Skip first 3 lines with metadata

    # Parse the lines and write them to the CSV
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # Write the CSV header row
        headers = ["Date_Time", "LAST_TIME_SYNC", "LAST_IF_PATH", "LAST_TRACK_ID", 
                   "LAST_WARM_START_STATUS", "LAST_AVG_SINR", "LAST_SINR_SAMPLES_RX", 
                   "LAST_VALID_SINRS_RX", "LAST_NULL_SINRS_RX", "LAST_ALARM", 
                   "LAST_RCM_TX_FREQ_REQ", "LAST_RCM_TX_FREQ_SET", "LAST_RCM_RX_FREQ_REQ", 
                   "LAST_RCM_RX_FREQ_SET", "TOTAL_TRACK_ADV_RX", "TOTAL_TRACK_ADV_RJ", 
                   "TOTAL_TRACK_CANCEL_RX", "TOTAL_TRACK_CANCEL_RJ", "TOTAL_TRACKS_RX", 
                   "TOTAL_TRACKS_RJ", "TOTAL_RCM_TX_REQ", "TOTAL_RCM_TX_SET", 
                   "TOTAL_RCM_RX_REQ", "TOTAL_RCM_RX_SET", "LAST_IPA_STATUS_P", 
                   "LAST_IPA_STATUS_S", "LAST_TN_OFFSET_P", "LAST_TN_OFFSET_S", 
                   "LAST_TARGET_AZ_P", "LAST_TARGET_AZ_S", "LAST_ACTUAL_AZ_P", 
                   "LAST_ACTUAL_AZ_S", "LAST_TARGET_EL_P", "LAST_TARGET_EL_S", 
                   "LAST_ACTUAL_EL_P", "LAST_ACTUAL_EL_S", "LAST_TARGET_CL_P", 
                   "LAST_TARGET_CL_S", "LAST_ACTUAL_CL_P", "LAST_ACTUAL_CL_S", 
                   "PID_WS", "TOTAL_ZOMBIES", "ENABLE_GAIN_EVT", "TOTAL_GAIN_EVTS_SENT", 
                   "TOTAL_AIM_CRASHES", "TOTAL_PCU_RESETS_P", "TOTAL_PCU_RESETS_S", 
                   "SINR_AVAILABLE", "LAST_CANCELLED_TRACK_ID", "TOTAL_ABNORMAL_TRACKS", 
                   "TOTAL_TRANSIT_TRACKS", "TOTAL_SEQUENTIAL_TRACKS", "LAST_TID_RJ", 
                   "TOTAL_PCU_CMDS_RTX_P", "TOTAL_PCU_CMDS_RTX_S", "TOTAL_PCU_CMDS_DROPPED_P", 
                   "TOTAL_PCU_CMDS_DROPPED_S", "TOTAL_INVALID_EL_TA", "TOTAL_INVALID_EL_TR", 
                   "TOTAL_SOFT_RESETS", "HOMING_STATUS_P", "HOMING_STATUS_S", 
                   "LAST_EL_OFFSET_P", "LAST_EL_OFFSET_S", "LAST_BFS_AZ_OFFSET_P", 
                   "LAST_BFS_EL_OFFSET_P", "LAST_BFS_AZ_OFFSET_S", "LAST_BFS_EL_OFFSET_S", 
                   "TX_MUTED_P", "TX_MUTED_S", "BLOCKAGE_STATUS_P", "LAST_BFS_CL_OFFSET_P", 
                   "LAST_BFS_CL_OFFSET_S"]
        writer.writerow(headers)

        # Process each line, combining date and time, then writing to CSV
        for line in lines:
            line_data = line.strip().split("\t")  # Split by tabs
            time = line_data[0]

            # Combine date and time in ISO 8601 format with milliseconds
            combined_datetime = f"{date}T{time}"
            row = [combined_datetime] + line_data[1:]
            writer.writerow(row)
    
    os.remove(input_file)
    print(f"Data has been successfully converted and saved to {output_file}")

# # Example usage
# convert_txt_to_csv('.\P_data\P_20240627_000000.txt')

#########END CONVERT CSV TO TXT#################

def convert1():
    dest_dir = extract_and_move()
    print(f"this is the dest dir: {dest_dir}")
    # Loop through all files in the folder
    for filename in os.listdir(dest_dir):
        print("inside")
        # Check if the file ends with .txt
        if filename.endswith('.txt'):
            file_path = os.path.join(dest_dir, filename)
            print(f"Processing file: {file_path}")
            convert_txt_to_csv(file_path)
    


    
def stitch_csv_files(file_list, output_file='stitched_output.csv'):
    # List to store each CSV data as a DataFrame
    data_frames = []
    
    #file_list = file_list.sort()
    file_list.sort()
    # Process each file in the list
    
    for file in file_list:
        file_path = os.path.join(os.getcwd(), file)
        # Read the CSV file, specifying the first column as 'Date_Time'
        df = pd.read_csv(file_path, parse_dates=['Date_Time'])
        
        # Append the DataFrame to the list (skipping headers)
        data_frames.append(df)
    
    # Concatenate all DataFrames, ignoring the index to reset row numbering
    stitched_df = pd.concat(data_frames, ignore_index=True)
    
    # Sort by 'Date_Time' column to maintain chronological order
    stitched_df.sort_values(by='Date_Time', inplace=True)
    
    # Save to a new CSV file
    stitched_df.to_csv(output_file, index=False)
    
    print(f"Files have been stitched together and saved as {output_file}")

# # Example usage
# file_list = ['P_20240627_005007.csv','P_20240627_000000.csv']
# stitch_csv_files(file_list, output_file='combined_logs.csv')

    


