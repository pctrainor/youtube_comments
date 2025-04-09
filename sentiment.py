import os
import sys
import json
import pandas as pd
from datetime import datetime
import openai
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import DefaultAzureCredential, ClientSecretCredential
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
OPENAI_API_KEY = ''
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_BLOB_CONTAINER = os.getenv("AZURE_BLOB_CONTAINER", "youtube-comments")
AZURE_TENANT_ID = os.getenv("AZURE_TENANT_ID")
AZURE_CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")
OUTPUT_FOLDER = "output"
AZURE_USE_CONNECTION_STRING = os.getenv("AZURE_USE_CONNECTION_STRING", "True").lower() == "true"

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
            # Use connection string if available
            return BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        else:
            # Use managed identity or other Azure credential
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

def upload_blob(container_client, blob_name, local_path):
    """Upload a file to a blob."""
    try:
        blob_client = container_client.get_blob_client(blob_name)
        with open(local_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        logger.info(f"Uploaded {local_path} to {blob_name}")
        return True
    except Exception as e:
        logger.error(f"Error uploading blob {local_path}: {e}")
        return False

def list_blob_files(container_client, prefix=""):
    """List all blobs with a given prefix."""
    try:
        blob_list = container_client.list_blobs(name_starts_with=prefix)
        return [blob.name for blob in blob_list]
    except Exception as e:
        logger.error(f"Error listing blobs with prefix {prefix}: {e}")
        return []

def get_video_metadata(container_client, video_id):
    """Get video metadata from Azure Blob Storage."""
    try:
        metadata_blob_name = f"{video_id}_metadata.csv"
        local_metadata_file = os.path.join(OUTPUT_FOLDER, metadata_blob_name)
        
        if download_blob_to_file(container_client, metadata_blob_name, local_metadata_file):
            metadata_df = pd.read_csv(local_metadata_file)
            if not metadata_df.empty:
                return metadata_df.iloc[0].to_dict()
        return None
    except Exception as e:
        logger.error(f"Error getting metadata for {video_id}: {e}")
        return None

def analyze_comments_with_openai(comments_df, video_metadata=None):
    """
    Analyze comments using OpenAI.
    
    Args:
        comments_df: DataFrame of comments
        video_metadata: Dictionary of video metadata
        
    Returns:
        str: Analysis text from OpenAI
    """
    if not OPENAI_API_KEY:
        logger.error("OpenAI API key not found. Please set OPENAI_API_KEY in your .env file.")
        return None

    # Basic sentiment analysis
    positive_count = 0
    negative_count = 0
    neutral_count = 0
    
    # Take a sample of comments (to manage token limits)
    sample_size = min(50, len(comments_df))
    sample_comments = comments_df.sample(n=sample_size) if sample_size > 0 else comments_df
    
    # Prepare video info for prompt
    if video_metadata:
        video_info = f"""
        Video Title: {video_metadata.get('title', 'Unknown')}
        Channel: {video_metadata.get('channelTitle', 'Unknown')}
        Published: {video_metadata.get('publishedAt', 'Unknown')}
        View Count: {video_metadata.get('viewCount', 'Unknown')}
        Like Count: {video_metadata.get('likeCount', 'Unknown')}
        Comment Count: {video_metadata.get('commentCount', 'Unknown')}
        """
    else:
        video_info = "Video metadata not available."
    
    # Prepare sample comments for the prompt
    comments_text = "\n".join([
        f"Comment {i+1}:\n"
        f"Text: {comment['text']}\n"
        f"Author: {comment['author']}\n"
        f"Likes: {comment['likeCount']}\n"
        for i, comment in enumerate(sample_comments.to_dict('records'))
    ])
    
    # Create prompt for OpenAI
    prompt = f"""
    Please analyze these YouTube video comments and provide insights:
    
    {video_info}
    
    SAMPLE COMMENTS (out of {len(comments_df)} total comments):
    {comments_text}
    
    Based on these comments, please provide:
    1. A summary of the overall viewer sentiment and key themes
    2. Notable patterns or trends in the comments
    3. Suggestions for the content creator based on viewer feedback
    4. Any issues or concerns that might need addressing
    """
    
    try:
        # Create OpenAI client
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
        
        # Call OpenAI API
        response = client.chat.completions.create(
            model="gpt-4", # Use appropriate model
            messages=[
                {"role": "system", "content": "You are an expert YouTube content analyst who helps creators understand their audience."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=1500
        )
        
        analysis = response.choices[0].message.content
        logger.info("OpenAI analysis completed successfully")
        return analysis
        
    except Exception as e:
        logger.error(f"Error calling OpenAI API: {e}")
        return None

def process_comment_file(container_client, blob_name):
    """
    Process a comment CSV file from Azure Blob Storage.
    
    Args:
        container_client: Azure blob container client
        blob_name: Name of the blob file (should be {video_id}_comments.csv)
        
    Returns:
        dict: Analysis results or None if failed
    """
    try:
        # Extract video_id from blob name
        video_id = blob_name.split('_')[0]
        
        # Download comments file
        local_file_path = os.path.join(OUTPUT_FOLDER, blob_name)
        if not download_blob_to_file(container_client, blob_name, local_file_path):
            return None
            
        # Read comments CSV
        comments_df = pd.read_csv(local_file_path)
        logger.info(f"Processing {len(comments_df)} comments for video {video_id}")
        
        # Get video metadata if available
        video_metadata = get_video_metadata(container_client, video_id)
        
        # Analyze comments using OpenAI
        openai_analysis = analyze_comments_with_openai(comments_df, video_metadata)
        
        if not openai_analysis:
            return None
            
        # Save analysis results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        analysis_filename = f"{video_id}_analysis_{timestamp}.txt"
        local_analysis_path = os.path.join(OUTPUT_FOLDER, 'sentiment_analysis', analysis_filename)
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_analysis_path), exist_ok=True)
        
        with open(local_analysis_path, 'w', encoding='utf-8') as f:
            f.write(openai_analysis)
        
        # Upload analysis to Azure Blob Storage
        blob_analysis_path = f"sentiment_analysis/{analysis_filename}"
        upload_blob(container_client, blob_analysis_path, local_analysis_path)
        
        logger.info(f"Analysis completed for {video_id}")
        return {
            'video_id': video_id,
            'analysis_file': analysis_filename,
            'comment_count': len(comments_df),
            'timestamp': timestamp
        }
        
    except Exception as e:
        logger.error(f"Error processing comment file {blob_name}: {e}")
        return None

def main():
    """Main function to process YouTube comments from Azure Blob Storage."""
    # Create local output directory
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    os.makedirs(os.path.join(OUTPUT_FOLDER, 'sentiment_analysis'), exist_ok=True)
    
    try:
        # Get blob service client
        blob_service_client = get_blob_service_client()
        
        # Get container client
        container_client = blob_service_client.get_container_client(AZURE_BLOB_CONTAINER)
        
        # List comment files to process
        comment_blobs = [blob for blob in list_blob_files(container_client) if blob.endswith('_comments.csv')]
        logger.info(f"Found {len(comment_blobs)} comment files to process")
        
        results = []
        for blob_name in comment_blobs:
            result = process_comment_file(container_client, blob_name)
            if result:
                results.append(result)
        
        # Save summary of processed files
        summary = {
            'processed_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'total_files': len(comment_blobs),
            'successful_analyses': len(results),
            'videos': results
        }
        
        summary_path = os.path.join(OUTPUT_FOLDER, 'sentiment_analysis', 'processing_summary.json')
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
            
        # Upload summary to Azure
        upload_blob(container_client, 'sentiment_analysis/processing_summary.json', summary_path)
        
        logger.info(f"Processing complete. {len(results)} of {len(comment_blobs)} files analyzed successfully.")
        return True
        
    except Exception as e:
        logger.error(f"Error in main processing: {e}")
        return False

if __name__ == "__main__":
    main()