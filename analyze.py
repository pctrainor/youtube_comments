import os
import sys
import glob
import json
import pandas as pd
import numpy as np
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
import openai
from dotenv import load_dotenv
from urllib.parse import urlparse, parse_qs
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import re

# Load environment variables
load_dotenv()
openai_api_key = os.getenv("OPENAI_API_KEY")
youtube_api_key = os.getenv("YOUTUBE_API_KEY")
OUTPUT_FOLDER = "output"

# Add this after your imports and before the other functions

def extract_video_id(url):
    """
    Extract YouTube video ID from various URL formats.
    
    Supported formats:
    - youtube.com/watch?v=VIDEO_ID
    - youtu.be/VIDEO_ID
    - youtube.com/shorts/VIDEO_ID
    
    Args:
        url (str): YouTube URL or video ID
        
    Returns:
        str: Video ID if found, None otherwise
    """
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
        print(f"Error extracting video ID: {e}")
        return None

# YouTube API client setup
def get_youtube_client():
    """Create and return an authenticated YouTube API client."""
    if not youtube_api_key:
        print("YouTube API key not found. Please set YOUTUBE_API_KEY in your .env file.")
        return None
    try:
        return googleapiclient.discovery.build('youtube', 'v3', developerKey=youtube_api_key)
    except Exception as e:
        print(f"Error creating YouTube client: {e}")
        return None

def fetch_video_metadata(video_id, output_folder):
    """
    Fetch metadata for a YouTube video and save to a CSV file.
    
    Args:
        video_id (str): YouTube video ID
        output_folder (str): Folder to save output file
        
    Returns:
        bool: True if successful, False otherwise
    """
    youtube = get_youtube_client()
    if not youtube:
        return False
        
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)
        output_file = os.path.join(output_folder, f"{video_id}_metadata.csv")
        
        # Make API request to get video details
        response = youtube.videos().list(
            part="snippet,statistics",
            id=video_id
        ).execute()
        
        if not response.get("items"):
            print(f"Video {video_id} not found or is not accessible.")
            return False
            
        # Extract relevant data
        video_info = response["items"][0]
        snippet = video_info["snippet"]
        statistics = video_info["statistics"]
        
        metadata = {
            'title': snippet["title"],
            'channelTitle': snippet["channelTitle"],
            'publishedAt': snippet["publishedAt"],
            'viewCount': statistics.get("viewCount", 0),
            'likeCount': statistics.get("likeCount", 0),
            'commentCount': statistics.get("commentCount", 0)
        }
        
        # Save to CSV
        pd.DataFrame([metadata]).to_csv(output_file, index=False)
        print(f"Saved video metadata to {output_file}")
        return True
        
    except HttpError as e:
        print(f"YouTube API error while fetching metadata: {e}")
        return False
    except Exception as e:
        print(f"Error fetching video metadata: {e}")
        return False

def fetch_video_comments(video_id, output_folder):
    """
    Fetch comments for a YouTube video and save to a CSV file.
    
    Args:
        video_id (str): YouTube video ID
        output_folder (str): Folder to save output file
        
    Returns:
        bool: True if successful, False otherwise
    """
    youtube = get_youtube_client()
    if not youtube:
        return False
        
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_folder, exist_ok=True)
        output_file = os.path.join(output_folder, f"{video_id}_comments.csv")
        
        # Fetch comments
        comments = []
        next_page_token = None
        
        # Limit number of comments to avoid quota issues
        max_comments = 500
        total_fetched = 0
        
        while total_fetched < max_comments:
            try:
                response = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=100,  # Max allowed by API
                    pageToken=next_page_token
                ).execute()
                
                if not response.get("items"):
                    break
                    
                for item in response["items"]:
                    comment = item["snippet"]["topLevelComment"]["snippet"]
                    comments.append({
                        'author': comment["authorDisplayName"],
                        'text': comment["textDisplay"],
                        'likeCount': comment["likeCount"],
                        'publishedAt': comment["publishedAt"]
                    })
                    total_fetched += 1
                    
                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break
                    
            except HttpError as e:
                if e.resp.status == 403 and "commentsDisabled" in str(e):
                    print("Comments are disabled for this video.")
                else:
                    print(f"YouTube API error: {e}")
                break
                
            except Exception as e:
                print(f"Error fetching comments: {e}")
                break
            
            # Stop if we've reached max comments
            if total_fetched >= max_comments:
                break
                
        if not comments:
            print("No comments found or comments are disabled for this video.")
            # Create an empty file anyway to indicate we tried
            pd.DataFrame(columns=['author', 'text', 'likeCount', 'publishedAt']).to_csv(output_file, index=False)
            return False
            
        # Save to CSV
        pd.DataFrame(comments).to_csv(output_file, index=False)
        print(f"Saved {len(comments)} comments to {output_file}")
        return True
        
    except Exception as e:
        print(f"Error in fetch_video_comments: {e}")
        return False

def run_data_fetching(video_id, output_folder):
    """
    Fetch data for a YouTube video and save to CSV files.
    
    Args:
        video_id (str): YouTube video ID
        output_folder (str): Folder to save output files
        
    Returns:
        bool: True if successful, False otherwise
    """
    print(f"Attempting to fetch data for video ID: {video_id}...")
    try:
        # Fetch metadata first
        metadata_success = fetch_video_metadata(video_id, output_folder)
        if not metadata_success:
            print("Failed to fetch video metadata.")
            return False
            
        # Then fetch comments
        comments_success = fetch_video_comments(video_id, output_folder)
        if not comments_success:
            print("Warning: Issues fetching comments, but we'll continue with analysis if possible.")
            
        # Check if files were created
        comment_file = os.path.join(output_folder, f"{video_id}_comments.csv")
        metadata_file = os.path.join(output_folder, f"{video_id}_metadata.csv")
        
        if os.path.exists(comment_file) and os.path.exists(metadata_file):
            print(f"Data fetching successful for {video_id}.")
            return True
        else:
            print(f"Data fetching completed, but some output files are missing for {video_id}.")
            return False
            
    except Exception as e:
        print(f"Error during data fetching for {video_id}: {e}")
        return False

# Download VADER lexicon if not already downloaded
try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    print("Downloading VADER lexicon...")
    nltk.download('vader_lexicon')

# Rest of your code...
# [Keep all your existing functions like extract_video_id, analyze_comment_sentiment, etc.]

# --- Main Execution Block ---
if __name__ == '__main__':
    # Create base output directory
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)

    if len(sys.argv) > 1:
        video_url_or_id = sys.argv[1]
        print(f"Processing input: {video_url_or_id}")

        video_id = None
        # Check if it looks like a URL
        if "youtube.com/" in video_url_or_id or "youtu.be/" in video_url_or_id:
            print("Input appears to be a URL. Extracting video ID...")
            video_id = extract_video_id(video_url_or_id)
            if video_id:
                print(f"Extracted Video ID: {video_id}")
            else:
                print(f"Error: Could not extract Video ID from URL: {video_url_or_id}")
                sys.exit(1) # Exit if ID extraction fails
        else:
            # Assume it's a direct video ID if it doesn't look like a URL
            print("Input does not look like a standard YouTube URL. Assuming it's a Video ID.")
            video_id = video_url_or_id

        if video_id:
             # Check if data files exist already
             comment_file_path = os.path.join(OUTPUT_FOLDER, f"{video_id}_comments.csv")
             metadata_file_path = os.path.join(OUTPUT_FOLDER, f"{video_id}_metadata.csv")

             if not os.path.exists(comment_file_path) or not os.path.exists(metadata_file_path):
                  print(f"\nWarning: Data files for video ID '{video_id}' not found in '{OUTPUT_FOLDER}'.")
                  print("Attempting to fetch data from YouTube API...")

                  # Use our implemented fetching function
                  success = run_data_fetching(video_id, OUTPUT_FOLDER)

                  if not success:
                      print(f"Error: Could not fetch data for video ID '{video_id}'.")
                      print("Please ensure you have a valid YouTube API key in your .env file.")
                      print("You may also check if the video exists and has public comments enabled.")
                      sys.exit(1) # Exit if data cannot be obtained
                  else:
                     print("Data fetching successful. Proceed to upload /sentiment_analysis comments to azure storage.")
        else:
             # Should not reach here if extraction check works properly
             print("Error: No valid video ID obtained.")
             sys.exit(1)

    else:
        print("Usage: python analyze.py <youtube_video_url_or_video_id>")
        print("\nExample usage:")
        print("  python analyze.py https://www.youtube.com/watch?v=dQw4w9WgXcQ")
        print("  python analyze.py dQw4w9WgXcQ")
        sys.exit(1) # Exit if no argument provided