#Blob configuration and upload code source: https://www.youtube.com/watch?v=enhJfb_6KYU&ab_channel=TechWithPat

import time
import os
import yaml
import pandas as pd
import logging
import boto3
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



def upload(files, aws_access_key_id, aws_secret_access_key, bucket_name, region):
    bucket_client = s3_resource.Bucket(bucket_name, region, aws_access_key_id, aws_secret_access_key)
    
    print("Uploading files to s3...")
    
    for file in files:
        boto_client = boto3.client(file.name.replace('.txt', file_tag))
        
        with open(file.path, "rb") as data:
            boto_client.upload_file(data, bucket_name, boto_client.name)
            print(f'{file.name} uploaded to s3 storage')
            

@time_efficiency_decorator
def download(destination, fnum, aws_access_key_id, aws_secret_access_key, bucket_name, region):
    """
    download process to test download speed
    """
    bucket_client = s3_resource.Bucket(bucket_name, region, aws_access_key_id, aws_secret_access_key)
    print("Downloading files from blob storage")
    file_tag = str(fnum)+'.txt'
    
    for file in bucket_client.objects.all():
        new_file = os.path.join(destination, file.name.replace('.txt', file_tag))
        with open(new_file, "wb") as f:
            s3.download_fileobj(bucket_name, new_file.name, f)
    

# Code initially written by Henry Tan, modified by Nicolas Wirth
if __name__ == '__main__':
    config = load_config()
    initialize_files()
    destination_folder = config["destination_download_folder"]
    source_folders = ["source_folder_1KB", "source_folder_1MB", "source_folder_10MB"]
    for a in source_folders:
        download_test = get_files(config[a])
        upload(download_test, config["aws_access_key_id"], 
                config["aws_secret_access_key"], config["downloadTest_bucket_name"], config["region"])   
    
    results = {
        'Trial': [], 
        'Time Taken': [], 
    }
    for i in range(100):
        time_taken = download(destination_folder, i, config["aws_access_key_id"], 
                config["aws_secret_access_key"], config["downloadTest_bucket_name"], config["region"])
        results['Trial'].append(i + 1)
        results['Time Taken'].append(time_taken)
    results_df = pd.DataFrame.from_dict(results)
    results_df.to_csv('download-test_results.csv', index=False)
    
