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
from tabulate import tabulate
from datetime import datetime
import csv

METADATA_SCHEMA_PATH = r'C:\Users\yaniv\Dropbox\Apps\yanivmalach\Macaque R24\jsonFormats\schema.json'
METADATA_FILE_PATH = r'C:\Users\yaniv\Dropbox\Apps\yanivmalach\Macaque R24\subject_metadata\sample.xlsx'
AIRR_SCHEMA_PATH = r'C:\Users\yaniv\Dropbox\Apps\yanivmalach\Macaque R24\jsonFormats\airr-schema.json'
GENOMIC_SCHEMA_PATH = r'C:\Users\yaniv\Dropbox\Apps\yanivmalach\Macaque R24\jsonFormats\genomic-schema.json'
CSV_FILE_PATH = r'C:\Users\yaniv\Dropbox\Apps\yanivmalach\Macaque R24\results\missing.csv'

WEBHOOK_URL = ''

class FolderMonitor(FileSystemEventHandler):
    def __init__(self, lock):
        self.new_subjects = []
        self.number_of_new_subjects = 0
        self.lock = lock
        self.passed_24 = 0
        self.missing_files_24 = 0
        self.missing_metadata_24 = 0
        self.total = 0
        self.csv_file = CSV_FILE_PATH

    def on_created(self, event):
        self.lock.acquire()
        time.sleep(1)
        self.new_subjects.append(event.src_path)
        self.number_of_new_subjects+=1
        print(f"New file added: {event.src_path}")
        self.total+=1
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

        with open(METADATA_SCHEMA_PATH, 'r') as schema_file:
            schema = json.load(schema_file)
            required_properties = schema['required']
        
            data = pd.read_excel(METADATA_FILE_PATH)
            row_number, missing_properties = self.scan_subject_metadata(data, required_properties, subject_name)
            result = self.scan_subject_files(subject_path)
            self.analyze_checks_result(row_number, missing_properties, result,subject_name)
        
    
    def analyze_checks_result(self, row_number, missing_properties, result,subject_name):
        m1 = m2 = m3 = ''
        if row_number == -1 or missing_properties != '' or result!= '':
            if row_number == -1 or missing_properties != '':
                self.missing_metadata_24 += 1
                m1 = f"Not found\n"
            else:
                m1 = f"Found in row {row_number}\n"

            if missing_properties!= '':
                m2 = f"{missing_properties}\n"

            for line in result:
                m3 += line
            
            with open(self.csv_file, mode='a', newline='') as csvfile:
                csv_writer = csv.writer(csvfile)
                csv_writer.writerow([subject_name, m1, m2, m3])


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
        missing = []
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

                miss_res = self.check_if_folder_meets_files_required(schema, folder, subject_path, sample)

                if miss_res:
                    missing.append(miss_res)
        
        return missing

    def check_if_folder_meets_files_required(self, schema, folder, subject_path, sample):
        #passed = ''
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
            #passed = f"{folder_short_name} passed."      
            self.passed_24 +=1
        else:
            self.missing_files_24+=1

        return missing_files

    def end_of_day_summery(self):
        with self.lock:
            total_24 = self.missing_files_24 + self.passed_24
            today = datetime.now().strftime("%B %d, %Y")  # Format the date as "Month Day, Year"
            table_data = [
            [f"{today}"],
            ["Total Subjects in System", self.total],
            ["Samples from the Past 24 Hours", total_24],
            ["Missing Files (Past 24 Hours)", self.missing_files_24],
            ["Missing Metadata (Past 24 Hours)", self.missing_metadata_24]
            ]
            table = tabulate(table_data, tablefmt="fancy_grid")
            self.send_slack_message("```\n" + table + "\n```")
            
            self.missing_metadata_24 = 0
            self.missing_files_24 = 0
            self.passed_24 = 0
        
    def send_slack_message(self, message):
        payload = {
            "text": message
        }
        response = requests.post(WEBHOOK_URL, json=payload)
