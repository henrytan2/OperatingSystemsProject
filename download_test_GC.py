#Blob configuration and upload code source: https://www.youtube.com/watch?v=enhJfb_6KYU&ab_channel=TechWithPat

import time
import os
import yaml
import pandas as pd
import logging
import boto3
from google.cloud import storage
from time_efficiency import time_efficiency_decorator

def load_config():
    dir_root = os.path.dirname(os.path.abspath(__file__))
    with open(dir_root + "/config_GoogleCloud.yml", "r") as yamlfile:
        return yaml.load(yamlfile, Loader=yaml.FullLoader)

def initialize_files():
    config = load_config()
    small_file_path = config["source_folder_1KB"]
    small_file_name = "small_file.txt"
    small_file = os.path.join(small_file_path, small_file_name)
    with open(small_file, 'wb') as f:
        f.seek(1024) # One KB
        f.write(b"\0")
        f.close()
    medium_file_path = config["source_folder_1MB"]
    medium_file_name = "medium_file.txt"
    medium_file = os.path.join(medium_file_path, medium_file_name)
    with open(medium_file, 'wb') as f:
        f.seek(1024 * 1024) # One MB
        f.write(b"\0")
        f.close()
    large_file_path = config["source_folder_10MB"]
    large_file_name = "large_file.txt"
    large_file = os.path.join(large_file_path, large_file_name)
    with open(large_file, 'wb') as f:
        f.seek(10 * 1024 * 1024) # Ten MB
        f.write(b"\0")
        f.close()

def get_files(dir):
    with os.scandir(dir) as entries:
        for entry in entries:
            if entry.is_file() and not entry.name.startswith('.'):
                yield entry



def upload(files, bucket_name):
    storage_client = storage.Client.from_service_account_json('clever-coast-307819-c139eade8d46.json')
    bucket_client = storage_client.get_bucket(bucket_name)
    
    print("Uploading files to Google Cloud Storage...")
    
    for file in files:
        blob_client = bucket_client.blob(file.name)
        
        with open(file.path, "rb") as data:
            blob_client.upload_from_filename(file.path)
            print(f'{file.name} uploaded to Google Cloud Storage')
            

@time_efficiency_decorator
def download(destination, fnum, bucket_name, fname):
    """
    download process to test download speed
    """
    storage_client = storage.Client.from_service_account_json('clever-coast-307819-c139eade8d46.json')
    bucket_client = storage_client.get_bucket(bucket_name)
    blob = bucket_client.blob(fname)
    print()
    print("Downloading files from Google Cloud storage")
    file_tag = str(fnum)+'.txt'
    new_file = os.path.join(destination, fname)
    blob.download_to_filename(new_file)
    

# Code initially written by Henry Tan, modified by Nicolas Wirth
if __name__ == '__main__':
    config = load_config()
    initialize_files()
    destination_folder = config["destination_download_folder"]
    source_folders = ["source_folder_1KB", "source_folder_1MB", "source_folder_10MB"]
    for a in source_folders:
        download_test = get_files(config[a])
        upload(download_test, config["downloadTest_bucket_name"])   
    
    results = {
        'Trial': [], 
        'Time Taken': [], 
        'File Size': [],
    }
    for n in range(100):
        if n == 0:
            file_name = "small_file.txt"
            file_size_string = ', 1KB'
        if n == 1:
            file_name = "medium_file.txt"
            file_size_string = ', 1MB'
        if n == 2:
            file_name = "large_file.txt"
            file_size_string = ', 10MB'
        for i in range(3):
            time_taken = download(destination_folder, i, config["downloadTest_bucket_name"], file_name)
            results['Trial'].append(i + 1)
            results['Time Taken'].append(time_taken)
            results['File Size'].append(file_size_string)
    results_df = pd.DataFrame.from_dict(results)
    results_df.to_csv('download-test_results_GC.csv', index=False)
    print("Test concluded successfully")
