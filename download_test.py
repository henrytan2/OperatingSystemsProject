#Blob configuration and upload code source: https://www.youtube.com/watch?v=enhJfb_6KYU&ab_channel=TechWithPat

import time
import os
import yaml
import pandas as pd
from azure.storage.blob import ContainerClient
from azure.storage.blob import BlobServiceClient
from time_efficiency import time_efficiency_decorator

def load_config():
    dir_root = os.path.dirname(os.path.abspath(__file__))
    with open(dir_root + "/config.yml", "r") as yamlfile:
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


def upload(files, connection_string, container_name):
    container_client = ContainerClient.from_connection_string(connection_string, container_name)
    print("Uploading files to blob storage...")
    
    for file in files:
        blob_client = container_client.get_blob_client(file.name)
        with open(file.path, "rb") as data:
            blob_client.upload_blob(data)
            print(f'{file.name} uploaded to blob storage')
            

@time_efficiency_decorator
def download(destination, fnum, connection_string, container_name):
    """
    download process to test download speed
    """
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = ContainerClient.from_connection_string(connection_string, container_name)
    print("Downloading files from blob storage")
    file_tag = str(fnum)+'.txt'
    
    blobs = container_client.list_blobs()
    for blob in blobs:
        blob_client = container_client.get_blob_client(blob)
        new_blob = os.path.join(destination, blob.name.replace('.txt', file_tag))
        with open(new_blob, "wb") as f:
            download_stream = blob_client.download_blob()
            f.write(download_stream.readall())
    

# Code initially written by Henry Tan, modified by Nicolas Wirth
if __name__ == '__main__':
    config = load_config()
    initialize_files()
    destination_folder = config["destination_download_folder"]
    source_folders = ["source_folder_1KB", "source_folder_1MB", "source_folder_10MB"]
    for a in source_folders:
        download_test = get_files(config[a])
        upload(download_test, config["azure_storage_connectionstring"], config["downloadTest_container_name"])   
    
    results = {
        'Trial': [], 
        'Time Taken': [], 
    }
    for i in range(100):
        time_taken = download(destination_folder, i, config["azure_storage_connectionstring"], config["downloadTest_container_name"])
        results['Trial'].append(i + 1)
        results['Time Taken'].append(time_taken)
    results_df = pd.DataFrame.from_dict(results)
    results_df.to_csv('download-test_results.csv', index=False)
    
