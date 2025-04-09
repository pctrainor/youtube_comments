from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

load_dotenv()

conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
container_name = os.getenv("AZURE_BLOB_CONTAINER")

try:
    # Create the BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    
    # Get container client
    container_client = blob_service_client.get_container_client(container_name)
    
    # List blobs in container
    print(f"Listing blobs in container {container_name}:")
    blobs_list = container_client.list_blobs()
    for blob in blobs_list:
        print(f"\t{blob.name}")
        
except Exception as e:
    print(f"Error: {str(e)}")