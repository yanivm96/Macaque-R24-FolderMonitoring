from flask import Flask, request
import dropbox
import time
import os
from dropbox.exceptions import AuthError
from FolderMonitor import FolderMonitor
from watchdog.observers import Observer

DROPBOX_ACCESS_TOKEN = 'sl.BjVwAHFObFMvO3DDcUQNNAk8FUJaPYEDcdzgBLAQ8fUT7KBD9BKjlg-7IDi8FW6UA0KUMY6sR7D2pZ0E8xmbmZeNr1Ixp3i2ZZA0sLZ3vvLQSOiTfY7PM8wB3EvgRui6pzeHfBam7qrSXYlUAj4J'
PATH = r'C:\Users\yaniv\Dropbox\Apps\yanivmalach\Macaque R24\Test\sample.xlsx'
MONITOR_FOLDER_PATH = r'C:\Users\yaniv\Dropbox\Apps\yanivmalach\Macaque R24\Test'
dbx = dropbox.Dropbox(DROPBOX_ACCESS_TOKEN)


def monitor_folder(path):
    event_handler = FolderMonitor()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    folder_path = MONITOR_FOLDER_PATH
    monitor_folder(folder_path)

