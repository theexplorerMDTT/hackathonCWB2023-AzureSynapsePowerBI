import os
from pathlib import Path
from azure.storage.blob import BlobServiceClient

# Get the connection String from Access Key in the Storage Account
connection_string = 'DefaultEndpointsProtocol=https;AccountName=datalakeb6mji41;AccountKey=16fZrA4Qu9fZpFMQqV5WqT4mYBO2KD0yLizcddfk9aCsMgEJRWWnO9iY4mtAV0g5KlLphavej5G7+AStMrRJJA==;EndpointSuffix=core.windows.net'

# Local directory containing the CSV files
local_directory = "/Users/yuko/Documents/DATA-PROJECTS/Hackathon-VertexVenture/AzData-EnrollmentDB/Output-CSV"

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


