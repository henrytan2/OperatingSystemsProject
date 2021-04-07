
import time
import os
import yaml
import pandas as pd
import logging
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


@time_efficiency_decorator
def upload(files, fnum, bucket_name, credentials_dict):
    """
    uploading files to test upload speed
    """
    #credentials = credentials_dict
    client = storage.Client(credentials=credentials_dict)
    bucket_client = client.get_bucket(bucket_name)
    
    print("Uploading files to s3...")
    file_tag = str(fnum)+'.txt'
    
    for file in files:
        blob_client = bucket_client.blob(file.name.replace('.txt', file_tag))
        
        with open(file.path, "rb") as data:
            blob_client.upload_from_filename(data)
            print(f'{file.name} uploaded to Google Cloud storage')
            

# Code initially written by Henry Tan, modified by Nicolas Wirth
if __name__ == '__main__':
    config = load_config()
    initialize_files()
    results = {
        'Trial': [], 
        'Time Taken': [], 
        'File Size': [],
    }
    for n in range(3):
        if n == 0:
            source_folder = "source_folder_1KB"
            file_size_string = ', 1KB'
        if n == 1:
            source_folder = "source_folder_1MB"
            file_size_string = ', 1MB'
        if n == 2:
            source_folder = "source_folder_10MB"
            file_size_string = ', 10MB'
        for i in range(100):
            upload_test = get_files(config[source_folder])
            time_taken = upload(upload_test, i, config["uploadTest_bucket_name"], config["Credentials_Dict"])
            results['Trial'].append(i + 1)
            results['Time Taken'].append(time_taken)
            results['File Size'].append(file_size_string)
    results_df = pd.DataFrame.from_dict(results)
    results_df.to_csv('upload-test_results.csv', index=False)
    
