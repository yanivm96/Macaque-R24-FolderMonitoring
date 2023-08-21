import time
import os
import warnings
import json
from dropbox.exceptions import AuthError
from FolderMonitor import FolderMonitor
from ServerDropbox import ServerDropbox
from watchdog.observers import Observer

warnings.simplefilter("ignore")
FOLDER_FOR_DOWNLOADS = r"/work/jenkins/Dropbox/Macaque R24/sequencing/"
DROPBOX_FOLDER_PATH = folder_path = '/macaque r24/sequencing'.lower()
SOURCE_PATH = r"/work/jenkins/Dropbox"
CURSOR_FILE_PATH = "cursor.json"  # Path to the file storing the cursor


def load_cursor(): #load dropbox last state
    if os.path.exists(CURSOR_FILE_PATH):
        with open(CURSOR_FILE_PATH, "r") as cursor_file:
            data = json.load(cursor_file)
            return data.get("cursor")
    return None

def save_cursor(cursor):#save dropbox state
    data = {"cursor": cursor}
    with open(CURSOR_FILE_PATH, "w") as cursor_file:
        json.dump(data, cursor_file)

def monitor_dropbox_folder():
    server_dropbox = ServerDropbox()
    folder_monitor = FolderMonitor()
    new_subjects = []
    cursor = load_cursor()  # Load cursor from the file
    result = start_new_check(server_dropbox, cursor, folder_monitor)
    print("finish daily run")
    cursor = result.cursor
    save_cursor(cursor)  # Save the cursor to the file

def start_new_check(server_dropbox, cursor, folder_monitor):
    new_subjects = []
    server_dropbox.connect_to_dropbox()  # if the token expired, set a new one
    if cursor is None:  # only for the first time
        result = server_dropbox.get_files_without_cursor(DROPBOX_FOLDER_PATH)  # Initial listing
    else:
        result = server_dropbox.get_files_with_cursor(cursor)  # Continue listing using cursor
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

    except Exception as e:
        print(e)

    return result

if __name__ == "__main__":
    monitor_dropbox_folder()
