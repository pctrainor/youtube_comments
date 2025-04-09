import os
import re
import logging
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential, ClientSecretCredential

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_BLOB_CONTAINER = os.getenv("AZURE_BLOB_CONTAINER", "youtube-comments")
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
OUTPUT_FOLDER = "download_blobs"
AZURE_USE_CONNECTION_STRING = os.getenv("AZURE_USE_CONNECTION_STRING", "True").lower() == "true"

def extract_video_id(url):
    """Extract YouTube video ID from various URL formats."""
    try:
        # Pattern for youtu.be and youtube.com/shorts URLs
        short_patterns = [
            r'youtu\.be/([a-zA-Z0-9_-]{11})',
            r'youtube\.com/shorts/([a-zA-Z0-9_-]{11})'
        ]
        
        # Check short URL patterns first
        for pattern in short_patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        
        # Parse URL for youtube.com/watch?v= format
        parsed_url = urlparse(url)
        if 'youtube.com' in parsed_url.netloc:
            query_params = parse_qs(parsed_url.query)
            if 'v' in query_params:
                return query_params['v'][0]
        
        # If it looks like a direct video ID, return it
        if re.match(r'^[a-zA-Z0-9_-]{11}$', url):
            return url
            
        return None
        
    except Exception as e:
        logger.error(f"Error extracting video ID: {e}")
        return None

def get_azure_credential():
    """Get Azure credential based on environment variables."""
    if AZURE_TENANT_ID and AZURE_CLIENT_ID and AZURE_CLIENT_SECRET:
        return ClientSecretCredential(
            tenant_id=AZURE_TENANT_ID,
            client_id=AZURE_CLIENT_ID,
            client_secret=AZURE_CLIENT_SECRET
        )
    return DefaultAzureCredential()

def get_blob_service_client():
    """Create a BlobServiceClient using connection string or managed identity."""
    try:
        if AZURE_USE_CONNECTION_STRING and AZURE_STORAGE_CONNECTION_STRING:
            return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        else:
            credential = get_azure_credential()
            account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL")
            if not account_url:
                raise ValueError("AZURE_STORAGE_ACCOUNT_URL is required when not using connection string")
            return BlobServiceClient(account_url=account_url, credential=credential)
    except Exception as e:
        logger.error(f"Error creating BlobServiceClient: {e}")
        raise

def download_blob_to_file(container_client, blob_name, local_path):
    """Download a blob to a local file."""
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # Download blob
        blob_client = container_client.get_blob_client(blob_name)
        with open(local_path, "wb") as file:
            data = blob_client.download_blob()
            file.write(data.readall())
        logger.info(f"Downloaded {blob_name} to {local_path}")
        return True
    except Exception as e:
        logger.error(f"Error downloading blob {blob_name}: {e}")
        return False

def main():
    """Main function to fetch YouTube comments or metadata from Azure Blob Storage."""
    # Create output directory
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    # Prompt user for YouTube URL or video ID
    user_input = input("Enter YouTube URL or video ID: ")
    
    # Extract video ID
    video_id = extract_video_id(user_input)
    if not video_id:
        logger.error("Could not extract a valid YouTube video ID")
        return False
    
    logger.info(f"Using video ID: {video_id}")
    
    # Ask user which file type they want
    file_type = input("Enter 'c' for comments or 'm' for metadata: ").lower()
    
    if file_type not in ['c', 'm']:
        logger.error("Invalid option. Must be 'c' or 'm'")
        return False
    
    try:
        # Connect to Azure Blob Storage
        blob_service_client = get_blob_service_client()
        container_client = blob_service_client.get_container_client(AZURE_BLOB_CONTAINER)
        
        # Set blob name based on file type choice
        blob_name = f"{video_id}_comments.csv" if file_type == 'c' else f"{video_id}_metadata.csv"
        local_path = os.path.join(OUTPUT_FOLDER, blob_name)
        
        # Download the file
        success = download_blob_to_file(container_client, blob_name, local_path)
        
        if success:
            logger.info(f"Successfully downloaded {blob_name} to {local_path}")
            return True
        else:
            logger.error(f"Failed to download {blob_name}")
            return False
            
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        return False

if __name__ == "__main__":
    main()