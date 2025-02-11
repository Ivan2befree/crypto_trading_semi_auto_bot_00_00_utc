import subprocess
import sys
import time
import datetime

def unix_to_human_time(timestamp):
    return datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')


def update_find_models_plot_open_final_folder_16():
    # Get the path to the Python interpreter executable
    interpreter = sys.executable
    files = ['update_historical_USDT_pairs_for_1D_next_bar_print_utc_time_00.py',
             'run_multiple_current_search_files.py',
            # 'execute_multiple_plot_files.py',
            # 'find_recent_jpeg_plot_files.py'
             ]

    for file in files:
        subprocess.run([interpreter, file])
if __name__=="__main__":
    start_time= time.time()
    update_find_models_plot_open_final_folder_16()
    end_time = time.time()
    overall_time = end_time - start_time
    print(f"start time={unix_to_human_time(start_time)}")
    print(f"end time={unix_to_human_time(end_time)}")
    print('overall time in minutes=', overall_time / 60.0)
    print('overall time in hours=', overall_time / 60.0 / 60.0)