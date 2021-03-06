#Blob configuration and upload code source: https://www.youtube.com/watch?v=enhJfb_6KYU&ab_channel=TechWithPat

import time
import os
import yaml
import pandas as pd
import logging
import boto3
from shutil import copyfile
from time_efficiency import time_efficiency_decorator

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


@time_efficiency_decorator
def upload(files, file_name, aws_access_key_id, aws_secret_access_key, bucket_name, region, folder):
    """
    uploading files to test upload speed
    """
    s3_resource = boto3.resource('s3')
    bucket_client = s3_resource.Bucket(bucket_name)
    
    print("Uploading files to s3...")
    # file_tag = str(fnum)+'.txt'
    
    boto_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    for file in files:   
        new_file = os.path.join(folder, file_name)
        boto_client.upload_file(new_file, bucket_name, file_name)
            

# Code initially written by Henry Tan, modified by Nicolas Wirth
if __name__ == '__main__':
    config = load_config()
    initialize_files()
    results = {
        'Trial': [], 
        'Time Taken': [], 
        'File Size': [],
    }
    file_name = ""
    for n in range(3):
        if n == 0:
            source_folder = "source_folder_1KB"
            file_name = 'small_file.txt'
            file_size_string = ', 1KB'
        if n == 1:
            source_folder = "source_folder_1MB"
            file_name = 'medium_file.txt'
            file_size_string = ', 1MB'
        if n == 2:
            source_folder = "source_folder_10MB"
            file_name = 'large_file.txt'
            file_size_string = ', 10MB'
        for i in range(100):
            upload_test = get_files(config[source_folder])
            time_taken = upload(upload_test, file_name, config["aws_access_key_id"], 
                config["aws_secret_access_key"], config["uploadTest_bucket_name"], config["region"], config[source_folder])
            results['Trial'].append(i + 1)
            results['Time Taken'].append(time_taken)
            results['File Size'].append(file_size_string)
    results_df = pd.DataFrame.from_dict(results)
    results_df.to_csv('upload-test_results_AWS.csv', index=False)
    
