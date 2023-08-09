from watchdog.events import FileSystemEventHandler
import pandas as pd
import jsonschema
import json
import os
import re
import requests
import argparse
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class FolderMonitor(FileSystemEventHandler):

    def on_created(self, event):
        print(f"New file added: {event.src_path}")
        #self.check_data(event.src_path)
        self.check_subject_metadata(event.src_path)

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

            print('subject row: ', row_number)
            if missing_properties!= '':
                print("subject missing:", missing_properties)
            
            for line in result:
                print(line)

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
        result = []

        FILES_SCHEMA_PATH = r'C:\Users\yaniv\Dropbox\Apps\yanivmalach\Macaque R24\jsonFormats\airr-FilesSchema.json'
        subject_folders = self.get_folders_in_path(subject_path)

        with open(FILES_SCHEMA_PATH, 'r') as schema_file:
            schema = json.load(schema_file)
            required_files = schema["items"]['required']

            for sample in subject_folders:
                sample_folders = self.get_folders_in_path(sample)
                for folder in sample_folders:
                    pass_res, miss_res = self.check_if_folder_meets_files_required(schema, folder, required_files, subject_path, sample)
                    if pass_res:
                        result.append(pass_res)
                    if miss_res:
                        result.append(miss_res)
        
        return result

    def check_if_folder_meets_files_required(self, schema, folder, required_files, subject_path, sample):
        passed = ''
        missing_files = ''
        
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
        
        # slack_message = f"Finished processing {file_name}: {meet_requirments_samples + missing_samples}  samples listed.\nThere are {meet_requirments_samples} new samples with metadata that meets the requirements.\nThere is {missing_samples} samples with metadata that does not meet the requirements."
        # print(slack_message)
        # self.send_slack_message(slack_message)
    
    def send_slack_message(self, message):
        webhook_url = 'https://hooks.slack.com/services/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'
        payload = {
            "text": message
        }
        response = requests.post(webhook_url, json=payload)
        print(response.text)





