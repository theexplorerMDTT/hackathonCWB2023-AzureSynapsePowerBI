import os
from pathlib import Path
from azure.storage.blob import BlobServiceClient

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
