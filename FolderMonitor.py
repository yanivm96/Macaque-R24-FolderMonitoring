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
Excel_FILE_PATH = r'C:\Users\yaniv\Dropbox\Apps\yanivmalach\Macaque R24\results\missing.xlsx'
WEBHOOK_URL = 'https://hooks.slack.com/services/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'
NOT_FOUND = -1
class FolderMonitor(FileSystemEventHandler):
    def __init__(self, lock):
        self.new_subjects = []
        self.number_of_new_subjects = 0
        self.lock = lock
        self.total_subjects = 0
        self.total_samples_airr = 0
        self.total_samples_genomic = 0
        self.air_samples_from_past_24 = 0
        self.genomic_samples_from_past_24 = 0
        self.airr_missing_files = 0
        self.genomic_missing_files = 0
        self.subjects_missing_metadata = 0
        self.isAirr = False
    
    def reset_counters_values(self):
        self.air_samples_from_past_24 = 0
        self.genomic_samples_from_past_24 = 0

    def on_created(self, event):
        self.lock.acquire()
        time.sleep(1)
        self.new_subjects.append(event.src_path)
        self.number_of_new_subjects+=1
        print(f"New file added: {event.src_path}")
        self.total_subjects+=1
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


    def check_new_subject(self, subject_path):
        # try:
        subject_name = self.get_file_name_from_file_path(subject_path)

        with open(METADATA_SCHEMA_PATH, 'r') as schema_file:
            schema = json.load(schema_file)
            required_properties = schema['required']
            data = pd.read_excel(METADATA_FILE_PATH)
            result = self.scan_subject_files(subject_path)
            row_number, missing_properties = self.scan_subject_metadata(data, required_properties, subject_name)
            self.analyze_checks_result(row_number, missing_properties, result,subject_name)
        # except Exception as e:
        #     print("error - ", e)

        
    
    def analyze_checks_result(self, row_number, missing_properties, result,subject_name):
        row_message = metadata_message = missing_files_message = ''
        missing_airr_files, missing_genomic_files = self.split_result_to_airr_and_genomic(result)
        row_message, metadata_message = self.analyze_metadata_check_results(row_number, missing_properties,metadata_message)

        if "Not found" in row_message or metadata_message or missing_airr_files or missing_genomic_files:
            data = {
                'Subject Name': [subject_name],
                'Row': [row_message],
                'Missing metadata properties': [metadata_message],
                'Missing files - Airr': [missing_airr_files],
                'Missing files - Genomic': [missing_genomic_files]
            }

            df = pd.DataFrame(data)
            with pd.ExcelWriter(Excel_FILE_PATH, engine='openpyxl', mode='a',if_sheet_exists="overlay") as writer:
                df.to_excel(writer, sheet_name="Sheet1",header=None, startrow=writer.sheets["Sheet1"].max_row,index=False)

    def split_result_to_airr_and_genomic(self, result):
        airr_lines = ""
        genomic_lines = ""

        for line in result:
            if 'airr' in line.lower():
                airr_lines += line + "\n"
            else:
                genomic_lines += line + "\n"

        return airr_lines, genomic_lines


    def analyze_metadata_check_results(self, row_number, missing_properties, metadata_message):
        missing_excel_data = False
        
        if row_number == NOT_FOUND:
            row_message = f"Not found\n"
            missing_excel_data = True
        else:
            row_message = f"Found in row {row_number}\n"

        if missing_properties != '':
            missing_excel_data = True
            metadata_message = f"{missing_properties}\n"   

        if missing_excel_data == True:
            self.subjects_missing_metadata += 1

        return row_message, metadata_message     

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
                    self.air_samples_from_past_24+=1
                    self.isAirr = True
                    schema = airr_schema
                else:
                    self.genomic_samples_from_past_24+=1
                    self.isAirr = False
                    schema = genomic_schema

                miss_res = self.check_if_folder_meets_files_required(schema, folder, subject_path, sample)

                if miss_res:
                    missing.append(miss_res)
        
        return missing

    def check_if_folder_meets_files_required(self, schema, folder, subject_path, sample):
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

        if len(missing_files) != 0:
            if self.isAirr == True:
                self.airr_missing_files+=1
            else:
                self.genomic_missing_files+=1

        return missing_files


    def end_of_day_summery(self):
        with self.lock:
            self.total_samples_airr +=  self.air_samples_from_past_24
            self.total_samples_genomic +=  self.genomic_samples_from_past_24
            table_data1, table_data2 = self.create_slack_table()
            table1 = tabulate(table_data1, tablefmt="fancy_grid")
            table2 = tabulate(table_data2, tablefmt="fancy_grid")
            self.send_slack_message("```\n" + table1 + "\n```")
            self.send_slack_message("```\n" + table2 + "\n```")
            self.reset_counters_values()


    def create_slack_table(self):
        today = datetime.now().strftime("%B %d, %Y")  # Format the date as "Month Day, Year"

        table1 = [
            [f"{today}", f"AIRR-Seq", f"Genomic"],
            ["Total Samples", f"{self.total_samples_airr}", f"{self.total_samples_genomic}"],
            ["Samples added in past 24 hours", f"{self.air_samples_from_past_24}", f"{self.genomic_samples_from_past_24}"],
            ["Samples missing files", f"{self.airr_missing_files}", f"{self.genomic_missing_files}"]
        ]

        table2 = [
            [f"{today}", f"AIRR-Seq", f"Genomic"],
            ["Total Subjects", f"{self.total_subjects}"],
            ["Subjects missing metadata", f"{self.subjects_missing_metadata}"]
        ]

        return table1, table2
    
    def send_slack_message(self, message):
        payload = {
            "text": message
        }
        response = requests.post(WEBHOOK_URL, json=payload)
