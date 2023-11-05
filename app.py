import time
import os
import warnings
import json
from dropbox.exceptions import AuthError
from FolderMonitor import FolderMonitor
from ServerDropbox import ServerDropbox
from watchdog.observers import Observer

# Ignore warnings
warnings.simplefilter("ignore")
CURSOR_FILE_PATH = "cursor.json"

# Define the file paths and Dropbox-related settings
SOURCE_PATH = r"C:\Users\yaniv\Desktop\Dropbox" # this is for testing
#SOURCE_PATH = r"/misc/work/jenkins/Dropbox"
FOLDER_FOR_DOWNLOADS = os.path.join(SOURCE_PATH, "Macaque R24/sequencing/")
DROPBOX_FOLDER_PATH = '/macaque r24/sequencing'.lower() #where the folder you want to monitor lacated in dropbox
DROPBOX_MISSING_FILE_PATH = '/macaque r24/results/missing.xlsx'.lower()
SERVER_MISSING_FILE_PATH = os.path.join(SOURCE_PATH, 'Macaque R24/results/missing.xlsx')
ALL_SAMPLES_PATH = os.path.join(SOURCE_PATH, 'Macaque R24/sequencing/all_samples_file.txt') 



# Function to load the last cursor state from a file
def load_cursor():
    if os.path.exists(CURSOR_FILE_PATH):
        with open(CURSOR_FILE_PATH, "r") as cursor_file:
            data = json.load(cursor_file)
            return data.get("cursor")
    return None

# Function to save the current cursor state to a file
def save_cursor(cursor):
    data = {"cursor": cursor}
    # with open(CURSOR_FILE_PATH, "w") as cursor_file:
    #     json.dump(data, cursor_file)

# Function to monitor Dropbox folder
def monitor_dropbox_folder():
    server_dropbox = ServerDropbox()  # Initialize ServerDropbox class
    folder_monitor = FolderMonitor()  # Initialize FolderMonitor class
    cursor = load_cursor()  # Load last cursor state
    result = start_new_check(server_dropbox, cursor, folder_monitor)
    print("finish daily run")
    cursor = result.cursor  # Update cursor
    save_cursor(cursor)  # Save new cursor state
    server_dropbox.update_file(DROPBOX_MISSING_FILE_PATH, SERVER_MISSING_FILE_PATH) # updating the missing file on dropbox

# Function to initiate a new Dropbox folder check
def start_new_check(server_dropbox, cursor, folder_monitor):
    past_day_subjects = []
    server_dropbox.connect_to_dropbox()  # Connect to Dropbox

    # Initial listing if no cursor found, otherwise continue with the last cursor
    if cursor is None:
        result = server_dropbox.get_files_without_cursor(DROPBOX_FOLDER_PATH)
    else:
        result = server_dropbox.get_files_with_cursor(cursor)

    try:
        if result.entries != []:
            for entry in result.entries:
                file_path = entry.path_display
                local_path = os.path.join(FOLDER_FOR_DOWNLOADS, entry.name)
                server_dropbox.download_entry(entry, local_path)
                
                if file_path in server_dropbox.download_list:
                    print(f"Finished download {file_path}")
                    past_day_subjects.append((SOURCE_PATH + file_path))

            print("Successfully downloaded all files")

        else:
            print("No new data from past day")
        
        folder_monitor.past_24_sample = server_dropbox.sample_list
        lines = open(ALL_SAMPLES_PATH, 'r').readlines()
        lines = [line.replace('\n', '') for line in lines]

        with open(ALL_SAMPLES_PATH, 'a') as append_all_samples_file:
            for subject_path in past_day_subjects:
                # Check if subject_path not in lines
                if subject_path not in lines:
                    lines.append(subject_path)
                    append_all_samples_file.write(subject_path + '\n')


        for subject_path in lines:
            if subject_path != '':
                folder_monitor.check_new_subject(subject_path) 
            
        
        folder_monitor.end_of_day_summary()
        folder_monitor.update_pipeline_table()

        #need to fix the pipline file

    except Exception as e:
        print(e)

    return result


def func():

    return 

# Main function
if __name__ == "__main__":
    monitor_dropbox_folder()
