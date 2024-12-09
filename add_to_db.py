import pandas as pd
import psycopg2
from psycopg2 import sql

table_name = 'log_data'
def upload_data_to_postgres(file_path, db_params):
    # Load the stitched CSV data into a DataFrame
    df = pd.read_csv(file_path)

    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    # Define the table name and create table SQL statement
    
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        date_time TIMESTAMP,
        last_time_sync INT,
        last_if_path INT,
        last_track_id INT,
        last_warm_start_status INT,
        last_avg_sinr INT,
        last_sinr_samples_rx INT,
        last_valid_sinrs_rx INT,
        last_null_sinrs_rx INT,
        last_alarm INT,
        last_rcm_tx_freq_req INT,
        last_rcm_tx_freq_set INT,
        last_rcm_rx_freq_req INT,
        last_rcm_rx_freq_set INT,
        total_track_adv_rx INT,
        total_track_adv_rj INT,
        total_track_cancel_rx INT,
        total_track_cancel_rj INT,
        total_tracks_rx INT,
        total_tracks_rj INT,
        total_rcm_tx_req INT,
        total_rcm_tx_set INT,
        total_rcm_rx_req INT,
        total_rcm_rx_set INT,
        last_ipa_status_p INT,
        last_ipa_status_s INT,
        last_tn_offset_p INT,
        last_tn_offset_s INT,
        last_target_az_p INT,
        last_target_az_s INT,
        last_actual_az_p INT,
        last_actual_az_s INT,
        last_target_el_p INT,
        last_target_el_s INT,
        last_actual_el_p INT,
        last_actual_el_s INT,
        last_target_cl_p INT,
        last_target_cl_s INT,
        last_actual_cl_p INT,
        last_actual_cl_s INT,
        pid_ws INT,
        total_zombies INT,
        enable_gain_evt INT,
        total_gain_evts_sent INT,
        total_aim_crashes INT,
        total_pcu_resets_p INT,
        total_pcu_resets_s INT,
        sinr_available INT,
        last_cancelled_track_id INT,
        total_abnormal_tracks INT,
        total_transit_tracks INT,
        total_sequential_tracks INT,
        last_tid_rj INT,
        total_pcu_cmds_rtx_p INT,
        total_pcu_cmds_rtx_s INT,
        total_pcu_cmds_dropped_p INT,
        total_pcu_cmds_dropped_s INT,
        total_invalid_el_ta INT,
        total_invalid_el_tr INT,
        total_soft_resets INT,
        homing_status_p INT,
        homing_status_s INT,
        last_el_offset_p INT,
        last_el_offset_s INT,
        last_bfs_az_offset_p INT,
        last_bfs_el_offset_p INT,
        last_bfs_az_offset_s INT,
        last_bfs_el_offset_s INT,
        tx_muted_p INT,
        tx_muted_s INT,
        blockage_status_p INT,
        last_bfs_cl_offset_p INT,
        last_bfs_cl_offset_s INT
    );
    """

    # Create the table
    cursor.execute(create_table_query)

    # Clear existing data in the table
    clear_data_query = f"DELETE FROM {table_name};"
    cursor.execute(clear_data_query)

    # Insert new data into the table
    for index, row in df.iterrows():
        insert_query = sql.SQL("""
            INSERT INTO {table} (date_time, last_time_sync, last_if_path, last_track_id, last_warm_start_status, last_avg_sinr,
            last_sinr_samples_rx, last_valid_sinrs_rx, last_null_sinrs_rx, last_alarm, last_rcm_tx_freq_req, last_rcm_tx_freq_set,
            last_rcm_rx_freq_req, last_rcm_rx_freq_set, total_track_adv_rx, total_track_adv_rj, total_track_cancel_rx,
            total_track_cancel_rj, total_tracks_rx, total_tracks_rj, total_rcm_tx_req, total_rcm_tx_set, total_rcm_rx_req,
            total_rcm_rx_set, last_ipa_status_p, last_ipa_status_s, last_tn_offset_p, last_tn_offset_s, last_target_az_p,
            last_target_az_s, last_actual_az_p, last_actual_az_s, last_target_el_p, last_target_el_s, last_actual_el_p,
            last_actual_el_s, last_target_cl_p, last_target_cl_s, last_actual_cl_p, last_actual_cl_s, pid_ws, total_zombies,
            enable_gain_evt, total_gain_evts_sent, total_aim_crashes, total_pcu_resets_p, total_pcu_resets_s, sinr_available,
            last_cancelled_track_id, total_abnormal_tracks, total_transit_tracks, total_sequential_tracks, last_tid_rj,
            total_pcu_cmds_rtx_p, total_pcu_cmds_rtx_s, total_pcu_cmds_dropped_p, total_pcu_cmds_dropped_s, total_invalid_el_ta,
            total_invalid_el_tr, total_soft_resets, homing_status_p, homing_status_s, last_el_offset_p, last_el_offset_s,
            last_bfs_az_offset_p, last_bfs_el_offset_p, last_bfs_az_offset_s, last_bfs_el_offset_s, tx_muted_p, tx_muted_s,
            blockage_status_p, last_bfs_cl_offset_p, last_bfs_cl_offset_s)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """).format(table=sql.Identifier(table_name))
        print(f"index: {index}, row: {row}")

        cursor.execute(insert_query, tuple(row))

    # Commit the changes and close the connection
    conn.commit()
    cursor.close()
    conn.close()
    
    print("Data uploaded to PostgreSQL successfully.")

# # Database connection parameters
# db_params = {
#     'host': 'postgresdb1.cr8iiqoue0l3.ap-southeast-2.rds.amazonaws.com',
#     'port': '5432',
#     'database': 'postgresDB1',
#     'user': 'postgres',
#     'password': 'your_password'  # Replace with your actual password
# }

# # Example usage
# file_path = 'stitched_output.csv'
# upload_data_to_postgres(file_path, db_params)
