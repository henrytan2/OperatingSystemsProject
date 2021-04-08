#Blob configuration and upload code source: https://www.youtube.com/watch?v=enhJfb_6KYU&ab_channel=TechWithPat

import time
import os
import yaml
import pandas as pd
import logging
import boto3
from time_efficiency import time_efficiency_decorator

s3_resource = boto3.resource("s3")
def load_config():
    dir_root = os.path.dirname(os.path.abspath(__file__))
    with open(dir_root + "/config_AWS.yml", "r") as yamlfile:
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



def upload(files, aws_access_key_id, aws_secret_access_key, bucket_name):
    #bucket_client = s3_resource.Bucket(bucket_name, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    
    print("Uploading files to s3...")
    boto_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    for file in files:
        new_file = file.path
        with open(file, "rb") as data:
            boto_client.upload_file(new_file, bucket_name, file.name)
            print(f'{file.name} uploaded to s3 storage')
            print()
            

@time_efficiency_decorator
def download(destination, fnum, aws_access_key_id, aws_secret_access_key, bucket_name, fname):
    """
    download process to test download speed
    """
    #bucket_client = s3_resource.Bucket(bucket_name, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    print()
    print("Downloading files from s3 storage")
    file_tag = str(fnum)+'.txt'
    boto_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    
    #for file in boto_client.objects.all():
    new_file = os.path.join(destination, fname)
        
    #with open(file, "wb") as f:
    boto_client.download_file(bucket_name, fname, new_file)
    print(f'{fname} downloaded from s3 storage')
    

# Code initially written by Henry Tan, modified by Nicolas Wirth
if __name__ == '__main__':
    config = load_config()
    initialize_files()
    destination_folder = config["destination_download_folder"]
    source_folders = ["source_folder_1KB", "source_folder_1MB", "source_folder_10MB"]
    for a in source_folders:
        download_test = get_files(config[a])
        upload(download_test, config["aws_access_key_id"], 
                config["aws_secret_access_key"], config["downloadTest_bucket_name"])   
    
    results = {
        'Trial': [], 
        'Time Taken': [], 
        'File Size': [],
    }
    for n in range(3):
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
            time_taken = download(destination_folder, i, config["aws_access_key_id"], 
                    config["aws_secret_access_key"], config["downloadTest_bucket_name"], file_name)
            results['Trial'].append(i + 1)
            results['Time Taken'].append(time_taken)
            results['File Size'].append(file_size_string)
    results_df = pd.DataFrame.from_dict(results)
    results_df.to_csv('download-test_results_AWS.csv', index=False)
    
