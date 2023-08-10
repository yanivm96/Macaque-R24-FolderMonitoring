from watchdog.events import FileSystemEventHandler
import pandas as pd
import jsonschema
import json
import os
import re
import requests
import time
import argparse
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading 


class FolderMonitor(FileSystemEventHandler):
    def __init__(self, lock):
        self.new_subjects = []
        self.number_of_new_subjects = 0
        self.lock = lock

    def on_created(self, event):
        self.lock.acquire()
        self.new_subjects.append(event.src_path)
        self.number_of_new_subjects+=1
        print(f"New file added: {event.src_path}")
        self.lock.release()
    
    

    def get_file_name_from_file_path(self, file_path):
        parts = file_path.split("\\")
        last_part = parts[-1]
        return last_part
    
    def get_folders_in_path(self, target_path):
        folders = []
        items = os.listdir(target_path)

        for item in items:
            item_path = os.path.join(target_path, item)
            if os.path.isdir(item_path):
                folders.append(item_path)
        
        return folders

    def check_subject_metadata(self, subject_path):
        subject_name = self.get_file_name_from_file_path(subject_path)
        METADATA_SCHEMA_PATH = r'C:\Users\yaniv\Dropbox\Apps\yanivmalach\Macaque R24\jsonFormats\schema.json'
        METADATA_FILE_PATH = r'C:\Users\yaniv\Dropbox\Apps\yanivmalach\Macaque R24\subject_metadata\sample.xlsx'

        with open(METADATA_SCHEMA_PATH, 'r') as schema_file:
            schema = json.load(schema_file)
            required_properties = schema['required']
        
            data = pd.read_excel(METADATA_FILE_PATH)
            row_number, missing_properties = self.scan_subject_metadata(data, required_properties, subject_name)
            result = self.scan_subject_files(subject_path)
            self.analyze_checks_result(row_number, missing_properties, result)
        
    
    def analyze_checks_result(self, row_number, missing_properties, result):
        slack_message = f'-------Starting to check a new sample-------\n'
        if row_number == -1:
            slack_message += f"subject was not found in the excel file\n"
        else:
            slack_message += f"subject found in row {row_number} in the excel file\n"

        if missing_properties!= '':
            slack_message += f"subject missing: , {missing_properties}\n"
        else:
            slack_message += f"subject has all required properties\n"

        print(slack_message)
        self.send_slack_message(slack_message)

        for line in result:
            self.send_slack_message(line)
            print(line)
        
        self.send_slack_message("-------Finished chicking a new sample-------")


    def scan_subject_metadata(self, data, required_properties, subject_name):
        missing_properties = ''
        row_number = -1
        for index, row in data.iterrows(): # Iterate through rows and write missing properties to the file
            if row['Animal ID'] == subject_name:
                row_number = index
                for req in required_properties:
                    if pd.isna(data[req][index]): #checking if cell is empty
                        missing_properties +=  req + ', '

                break #finish after finding the row of the subject and checking the metadata
        
        return row_number, missing_properties
        

    def scan_subject_files(self,subject_path):
        AIRR_SCHEMA_PATH = r'C:\Users\yaniv\Dropbox\Apps\yanivmalach\Macaque R24\jsonFormats\airr-schema.json'
        GENOMIC_SCHEMA_PATH = r'C:\Users\yaniv\Dropbox\Apps\yanivmalach\Macaque R24\jsonFormats\genomic-schema.json'
        result = []
        schema = None 
        subject_folders = self.get_folders_in_path(subject_path)
        airr_schema = json.load(open(AIRR_SCHEMA_PATH, 'r'))
        genomic_schema = json.load(open(GENOMIC_SCHEMA_PATH, 'r'))

        for sample in subject_folders:
            sample_folders = self.get_folders_in_path(sample)
            for folder in sample_folders:
                if "airr" in self.get_file_name_from_file_path(folder):
                    schema = airr_schema
                else:
                    schema = genomic_schema

                pass_res, miss_res = self.check_if_folder_meets_files_required(schema, folder, subject_path, sample)

                if pass_res:
                    result.append(pass_res)
                if miss_res:
                    result.append(miss_res)
        
        return result

    def check_if_folder_meets_files_required(self, schema, folder, subject_path, sample):
        passed = ''
        missing_files = ''
        required_files = schema["items"]['required']

        for required_file in required_files:
            actual_count = 0
            expected_count = schema["items"]["properties"][required_file + "_count"]["minimum"]
            for filename in os.listdir(folder):
                pattern = schema["items"]["properties"][required_file].get("pattern")
                if pattern and re.match(pattern, filename):
                    actual_count += 1
            
            subject_name = self.get_file_name_from_file_path(subject_path)
            sample_name = self.get_file_name_from_file_path(sample)
            folder_name = self.get_file_name_from_file_path(folder)
            folder_short_name = f"{subject_name}/{sample_name}/{folder_name}"
            
            if actual_count < expected_count:
                missing_files +=f"{folder_short_name}: Expected {expected_count} file of type **{required_file}**, found only {actual_count}\n"

        if missing_files.endswith('\n'): #removing the lask \n 
            missing_files = missing_files[:-1]

        if len(missing_files) == 0:
            passed = f"{folder_short_name} passed."      

        return passed, missing_files

    
    def send_slack_message(self, message):
        webhook_url = ''
        payload = {
            "text": message
        }
        response = requests.post(webhook_url, json=payload)
