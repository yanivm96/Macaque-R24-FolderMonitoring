import dropbox
import time
import os
import warnings
from dropbox.exceptions import AuthError
from FolderMonitor import FolderMonitor
from watchdog.observers import Observer
import threading 
import asyncio


warnings.simplefilter("ignore")
DROPBOX_ACCESS_TOKEN = ''
FOLDER_FOR_DOWNLOADS = r"/home/bcrlab/malachy7/Dropbox/Macaque R24/sequencing/"
SOURCE_PATH = r"/home/bcrlab/malachy7"

dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
lock = threading.Lock()


def download_entry(dbx, entry, local_path):
    try:
        if isinstance(entry, dropbox.files.FileMetadata):
            with open(local_path, 'wb') as f:
                metadata, res = dbx.files_download(entry.path_display)
                f.write(res.content)
        elif isinstance(entry, dropbox.files.FolderMetadata):
            # For folders, create a corresponding local folder
            os.makedirs(local_path, exist_ok=True)
            # Download the contents of the folder recursively
            download_folder_contents(dbx, entry.path_display, local_path)
            
    except dropbox.exceptions.HttpError as e:
        print(f"Error downloading {entry.path_display}: {e}")

def download_folder_contents(dbx, folder_path, local_path):
    # List the contents of the folder
    result = dbx.files_list_folder(folder_path)
    for entry in result.entries:
        entry_local_path = os.path.join(local_path, entry.name)
        download_entry(dbx, entry, entry_local_path)

def monitor_dropbox_folder():
    event_handler = FolderMonitor(lock)
    new_subjects = []
    dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
    folder_path = '/macaque r24/sequencing'.lower()
    cursor = None
    number_of_files = 0
    while True:
        if cursor is None:
            # Initial listing
            result = dbx.files_list_folder(folder_path)
        else:
            # Continue listing using cursor
            result = dbx.files_list_folder_continue(cursor)
        try:
            if result.entries != []:
                for entry in result.entries:
                    file_path = entry.path_display
                    local_path = os.path.join(FOLDER_FOR_DOWNLOADS, entry.name)
                    print(f"Starting to download {file_path}")
                    download_entry(dbx, entry, local_path)
                    print(f"Finished download {file_path}")
                    new_subjects.append((SOURCE_PATH + file_path))

                print("success to download all files")
                for subject_path in new_subjects:
                    event_handler.check_new_subject(subject_path)

                new_subjects.clear()
            event_handler.end_of_day_summery()
        except Exception as e:
                print(e)
        
        cursor = result.cursor
        time.sleep(30)# Sleep for a while before checking again


if __name__ == "__main__":
    monitor_dropbox_folder()
