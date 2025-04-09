# YouTube Comments Analyzer

A tool that downloads YouTube comments, analyzes sentiment using OpenAI, and stores results in Azure Blob Storage.

## Required API Keys and Configuration

This application requires three sets of credentials:

1. YouTube Data API key
2. OpenAI API key
3. Azure Blob Storage configuration

### YouTube API Setup

1. Go to the [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or select an existing one)
3. Navigate to "APIs & Services" > "Library"
4. Search for "YouTube Data API v3" and enable it
5. Go to "APIs & Services" > "Credentials"
6. Click "Create Credentials" > "API Key"
7. Copy your new API key

### OpenAI API Setup

1. Visit [OpenAI's Platform](https://platform.openai.com/api-keys)
2. Create an account or sign in
3. Navigate to API Keys section
4. Click "Create new secret key"
5. Copy your API key

### Azure Blob Storage Setup

1. Go to [Azure Portal](https://portal.azure.com)
2. Create a new Storage Account or select existing one
3. Under "Security + networking" > "Access keys"
4. Click "Show keys" and copy:
   - Connection string
   - Account URL

### Reddit API Setup

1. Go to [Reddit's App Preferences](https://www.reddit.com/prefs/apps)
2. Scroll to the bottom and click "create another app..."
3. Fill in the following details:
   - Name: Your application name
   - Select "script" as the application type
   - Description: Brief description of your app
   - About URL: Your website or GitHub repository (optional)
   - Redirect URI: Use `http://localhost:8080`
4. Click "create app"
5. Copy the following information:
   - Client ID (displayed under your app name)
   - Client Secret (labeled as "secret")
   - User Agent (format as: `platform:app_name:version by /u/username`)

### Configuration

Create a `.env` file in the root directory with the following content:

```env
# YouTube API Credentials
YOUTUBE_API_KEY=your_youtube_api_key_here

# OpenAI Configuration
OPENAI_API_KEY=your_openai_api_key_here

# Azure Storage Configuration
AZURE_STORAGE_CONNECTION_STRING=your_connection_string_here
AZURE_BLOB_CONTAINER=youtube-comments
AZURE_STORAGE_ACCOUNT_URL=your_account_url_here
```

## Usage

1. First, download comments from a YouTube video:

```bash
python analyze.py https://www.youtube.com/watch?v=VIDEO_ID
```

2. Then analyze the comments using OpenAI and store in Azure:

```bash
python sentiment.py
```

## Notes

- YouTube Data API has a quota limit of 10,000 units per day
- OpenAI API usage is billed based on tokens processed
- Azure Blob Storage has storage and transaction costs
- All API keys should be kept secure and never committed to source control
