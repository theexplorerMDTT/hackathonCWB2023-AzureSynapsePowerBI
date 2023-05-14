import os
from pathlib import Path
from azure.storage.blob import BlobServiceClient

######################## DOWNLOAD MDB FILE FROM AZURE BLOB STORAGE######################## 

#Get the connection String from Access Key in the Storage Account
connection_String ='DefaultEndpointsProtocol=https;AccountName=datalakeb6mji41;AccountKey=16fZrA4Qu9fZpFMQqV5WqT4mYBO2KD0yLizcddfk9aCsMgEJRWWnO9iY4mtAV0g5KlLphavej5G7+AStMrRJJA==;EndpointSuffix=core.windows.net'


# Name of the container in Azure Storage
container_name = "files"

# Get the current working directory
current_directory = os.getcwd()

# Create a new directory for isolation within the current working directory
new_directory = os.path.join(current_directory, "AzData-EnrollmentDB")

# Create the directory if it doesn't exist
if not os.path.exists(new_directory):
    os.makedirs(new_directory)

# Create a BlobServiceClient instance
blob_service_client = BlobServiceClient.from_connection_string(connection_String)

# Get a BlobContainerClient instance for the specific container
container_client = blob_service_client.get_container_client(container_name)

# Get a list of all blobs in the container
blob_list = container_client.list_blobs()


# Iterate over the blobs and download the ones with .mdb extension
for blob in blob_list:
    if blob.name.endswith(".mdb"):
        # Construct the local file path to save the downloaded file
        local_file_path = os.path.join(new_directory, os.path.basename(blob.name))

        # Download the blob to the local machine
        with open(local_file_path, "wb") as file:
            file.write(container_client.download_blob(blob).readall())

        print(f"Downloaded blob: {blob.name} to: {local_file_path}")

######################## CONVERT MDB to CSV TABLES ######################## 

import subprocess
import os

# Specify the path to the MDB file
mdb_file = "{local_file_path}/ENROLL2022_20221114.mdb"

# Extract the directory path from the MDB file
mdb_directory = os.path.dirname(mdb_file)

# Create the output folder within the directory path if it doesn't exist
output_folder = os.path.join(mdb_directory, "Output-CSV")
os.makedirs(output_folder, exist_ok=True)

# Get a list of table names in the MDB file
cmd_get_tables = f"mdb-tables -1 {mdb_file}"
tables = subprocess.check_output(cmd_get_tables, shell=True, text=True).splitlines()

# Loop through each table and export it to CSV
for table_name in tables:
    print(table_name)


    if not table_name:
        continue  # Skip empty table names

    # Generate the output CSV file path based on the table name
    file_name = table_name.replace('/', '')  # Remove slash
    csv_file = os.path.join(output_folder, f"{file_name.replace(' ', '_')}.csv")

    # Export the table to CSV
    cmd_export_table = f"mdb-export '{mdb_file}' '{table_name}' > '{csv_file}'"
    subprocess.run(cmd_export_table, shell=True)

    print(f"Exported table: {table_name}")

print("Conversion completed successfully.")


######################## UPLOAD CSV FILE TO AZURE BLOB STORAGE ######################## 


# Local directory containing the CSV files
local_directory = "{output_folder}"

# Name of the container in Azure Storage
container_name = "files"

# Create a BlobServiceClient instance
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Get a BlobContainerClient instance for the specified container
container_client = blob_service_client.get_container_client(container_name)

# Specify the name of the new directory
new_directory = "/data/converted-csv"

# List all files in the local directory
files_uploaded = False  # Flag to track if any files are uploaded

# List all files in the local directory
for file_name in os.listdir(local_directory):
    if file_name.endswith(".csv"):
        # Upload each CSV file to Azure Blob Storage
        local_file_path = os.path.join(local_directory, file_name)
        #blob_client = container_client.get_blob_client(blob=container_name + "/" + file_name)
        blob_client = container_client.get_blob_client(blob= new_directory + "/" + file_name)

        with open(local_file_path, "rb") as file:
            blob_client.upload_blob(file, overwrite=True)  # Set overwrite to True

        # Get the uploaded blob URL
        blob_url = blob_client.url
        print(f"Uploaded file: {file_name}")
        print(f"Blob URL: {blob_url}")

        files_uploaded = True

if files_uploaded:
    print("Upload completed.")
else:
    print("No CSV files found for upload.")


