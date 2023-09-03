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

# Define the file paths and Dropbox-related settings
CURSOR_FILE_PATH = "cursor.json"
#FOLDER_FOR_DOWNLOADS = r"/work/jenkins/Dropbox/Macaque R24/sequencing/"
#DROPBOX_FOLDER_PATH = folder_path = '/macaque r24/sequencing'.lower()
#SOURCE_PATH = r"/work/jenkins/Dropbox"
FOLDER_FOR_DOWNLOADS = r"C:\Users\yaniv\Desktop\Dropbox\Macaque R24\sequencing"
DROPBOX_FOLDER_PATH = folder_path = '/macaque r24/sequencing'.lower()
SOURCE_PATH = r"C:\Users\yaniv\Desktop\Dropbox"

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
    with open(CURSOR_FILE_PATH, "w") as cursor_file:
        json.dump(data, cursor_file)

# Function to monitor Dropbox folder
def monitor_dropbox_folder():
    server_dropbox = ServerDropbox()  # Initialize ServerDropbox class
    folder_monitor = FolderMonitor()  # Initialize FolderMonitor class
    cursor = load_cursor()  # Load last cursor state
    result = start_new_check(server_dropbox, cursor, folder_monitor)
    print("finish daily run")
    cursor = result.cursor  # Update cursor
    save_cursor(cursor)  # Save new cursor state

# Function to initiate a new Dropbox folder check
def start_new_check(server_dropbox, cursor, folder_monitor):
    new_subjects = []
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
                print(f"Starting to download {file_path}")
                server_dropbox.download_entry(entry, local_path)
                print(f"Finished download {file_path}")
                new_subjects.append((SOURCE_PATH + file_path))

            print("Successfully downloaded all files")
            
            for subject_path in new_subjects:
                folder_monitor.check_new_subject(subject_path)
        else:
            print("No new data from past day")
        
        folder_monitor.end_of_day_summary()
        folder_monitor.update_pipeline_table()

    except Exception as e:
        print(e)

    return result

# Main function
if __name__ == "__main__":
    monitor_dropbox_folder()
