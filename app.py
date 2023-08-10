import dropbox
import time
import os
import warnings
from dropbox.exceptions import AuthError
from FolderMonitor import FolderMonitor
from watchdog.observers import Observer
import threading 

warnings.simplefilter("ignore")
DROPBOX_ACCESS_TOKEN = 'sl.BjVwAHFObFMvO3DDcUQNNAk8FUJaPYEDcdzgBLAQ8fUT7KBD9BKjlg-7IDi8FW6UA0KUMY6sR7D2pZ0E8xmbmZeNr1Ixp3i2ZZA0sLZ3vvLQSOiTfY7PM8wB3EvgRui6pzeHfBam7qrSXYlUAj4J'
MONITOR_FOLDER_PATH = r'C:\Users\yaniv\Dropbox\Apps\yanivmalach\Macaque R24\sequencing'
dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)
lock = threading.Lock()

def monitor_folder(path):
    event_handler = FolderMonitor(lock)
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(15)
            with lock:
                len = event_handler.number_of_new_subjects
                event_handler.number_of_new_subjects = 0
                new_subjects_copy = event_handler.new_subjects[:]  # Copy the list
                event_handler.new_subjects.clear()

            for src_path in new_subjects_copy:
                event_handler.check_subject_metadata(src_path)

    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    folder_path = MONITOR_FOLDER_PATH
    monitor_folder(folder_path)

