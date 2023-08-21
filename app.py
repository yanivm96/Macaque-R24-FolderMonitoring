import time
import os
import warnings
from dropbox.exceptions import AuthError
from FolderMonitor import FolderMonitor
from ServerDropbox import ServerDropbox
from watchdog.observers import Observer
import threading


warnings.simplefilter("ignore")
FOLDER_FOR_DOWNLOADS = r"/home/bcrlab/malachy7/Dropbox/Macaque R24/sequencing/"
DROPBOX_FOLDER_PATH = folder_path = '/macaque r24/sequencing'.lower()
SOURCE_PATH = r"/home/bcrlab/malachy7/Dropbox"
RUN_SERVER = True
TIME_BETWEEN_CHECKS = 30
lock = threading.Lock()

def monitor_dropbox_folder():
    server_dropbox = ServerDropbox()
    folder_monitor = FolderMonitor(lock)
    new_subjects = []
    cursor = None
    number_of_files = 0
    while RUN_SERVER:
        result = start_new_check(server_dropbox, cursor, folder_monitor)
        print("finish daily run")
        cursor = result.cursor
        time.sleep(TIME_BETWEEN_CHECKS)# Sleep for a while before checking again

def start_new_check(server_dropbox, cursor, folder_monitor):
    new_subjects = []
    server_dropbox.connect_to_dropbox() #if the token expired sets a new one
    if cursor is None:#only for the first time
        result = server_dropbox.get_files_without_cursor(DROPBOX_FOLDER_PATH)# Initial listing
    else:
        result = server_dropbox.get_files_with_cursor(cursor)# Continue listing using cursor
    try:
        if result.entries != []:
            for entry in result.entries:
                file_path = entry.path_display
                local_path = os.path.join(FOLDER_FOR_DOWNLOADS, entry.name)
                print(f"Starting to download {file_path}")
                server_dropbox.download_entry(entry, local_path)
                print(f"Finished download {file_path}")
                new_subjects.append((SOURCE_PATH + file_path))

            print("success to download all files")
            for subject_path in new_subjects:
                folder_monitor.check_new_subject(subject_path)

        else:
            print("no new data from past day")
        folder_monitor.end_of_day_summery()

    except Exception as e:
            print(e)
    
    return result;

if __name__ == "__main__":
    monitor_dropbox_folder()
