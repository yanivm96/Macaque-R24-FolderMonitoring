from watchdog.events import FileSystemEventHandler
import pandas as pd
import jsonschema
import json
import os
import requests
import argparse
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class FolderMonitor(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        print(f"New file added: {event.src_path}")
        self.check_data(event.src_path)

    def get_file_name_from_file_path(self, file_path):
        parts = file_path.split("\\")
        last_part = parts[-1]
        return last_part

    def check_data(self, file_path):
        file_name = self.get_file_name_from_file_path(file_path)
        MISSING_FILE_PATH = r'C:\Users\yaniv\Dropbox\Apps\yanivmalach\Macaque R24\Result\missing_samples_of ' + os.path.splitext(file_name)[0] 
        JSON_SCHEMA_PATH = r'C:\Users\yaniv\Dropbox\Apps\yanivmalach\Macaque R24\jsonFormats\schema.json'

        with open(JSON_SCHEMA_PATH, 'r') as schema_file:
            schema = json.load(schema_file)
            required_properties = schema['required']

        data = pd.read_excel(file_path)
        missing_file = open((MISSING_FILE_PATH), 'w')
        self.scan_exel_file(data, required_properties, missing_file, file_name)


    def scan_exel_file(self, data,required_properties, missing_file, file_name):
        meet_requirments_samples = 0 
        missing_samples = 0

        for index, row in data.iterrows(): # Iterate through rows and write missing properties to the file
            missing_properties = ''

            for req in required_properties:
                if pd.isna(data[req][index]): #checking if cell is empty
                    missing_properties +=  req + ', '
            
            if missing_properties != '':
                missing_samples+=1
                missing_message = f"Missing properties in row {index}: {missing_properties}"
                missing_file.write((str)(missing_message + '\n'))
            else:
                meet_requirments_samples+=1
                self.proggress_row_data(row)

        missing_file.close()
        slack_message = f"Finished processing {file_name}: {meet_requirments_samples + missing_samples}  samples listed.\nThere are {meet_requirments_samples} new samples with metadata that meets the requirements.\nThere is {missing_samples} samples with metadata that does not meet the requirements."
        print(slack_message)
        self.send_slack_message(slack_message)
    
    def proggress_row_data (self, row):
        animal_id = row['Animal ID']
        center = row['Center']
        species = row['Species']
        sub_species = row['Sub-species']
        sex = row['Sex']
        dob = row['Date of Birth (YYYY-MM-DD)']

    def send_slack_message(self, message):
        webhook_url = 'https://hooks.slack.com/services/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'
        payload = {
            "text": message
        }
        response = requests.post(webhook_url, json=payload)
        print(response.text)






