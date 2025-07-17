# Import required modules
import os
import json
from google.oauth2.service_account import Credentials  # For Google service account authentication
from googleapiclient.discovery import build            # To build the Google Drive service client
from googleapiclient.http import MediaFileUpload       # For uploading files to Drive
from datetime import datetime, timedelta               # To handle date and time

# Define a class to manage saving files to Google Drive (specific for Camping use case)
class SavingOnDriveCamping:
    def __init__(self, credentials_dict):
        self.credentials_dict = credentials_dict  # Dictionary containing Google service account credentials
        self.scopes = ['https://www.googleapis.com/auth/drive']  # Required Drive API scopes
        self.service = None  # Will hold the authenticated Drive API client
        self.parent_folder_id = '1K-p_VbH_WmUWPRKkiVsnvZji_ZAtAHz1'  # The ID of the main parent folder on Google Drive

    def authenticate(self):
        """Authenticate with Google Drive API using service account."""
        try:
            print("Authenticating with Google Drive...")
            # Use credentials dictionary to authenticate
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)
            # Build the Google Drive service client
            self.service = build('drive', 'v3', credentials=creds)
            print("Authentication successful.")
        except Exception as e:
            print(f"Authentication error: {e}")
            raise  # Re-raise the exception to let caller handle it

    def get_folder_id(self, folder_name):
        """Get folder ID by name inside the parent folder."""
        try:
            # Query to search for a folder with the given name under the specified parent folder
            query = (f"name='{folder_name}' and "
                     f"'{self.parent_folder_id}' in parents and "
                     f"mimeType='application/vnd.google-apps.folder' and "
                     f"trashed=false")
            
            # Execute the search
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'  # Only fetch id and name
            ).execute()
            
            files = results.get('files', [])
            if files:
                # If folder found, return its ID
                print(f"Folder '{folder_name}' found with ID: {files[0]['id']}")
                return files[0]['id']
            else:
                # Folder not found
                print(f"Folder '{folder_name}' does not exist.")
                return None
        except Exception as e:
            print(f"Error getting folder ID: {e}")
            return None

    def create_folder(self, folder_name):
        """Create a new folder under the parent folder on Drive."""
        try:
            print(f"Creating folder '{folder_name}'...")
            # Define metadata for new folder
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',  # Set type to folder
                'parents': [self.parent_folder_id]  # Set parent folder
            }
            # Create the folder and return its ID
            folder = self.service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()
            print(f"Folder '{folder_name}' created with ID: {folder.get('id')}")
            return folder.get('id')
        except Exception as e:
            print(f"Error creating folder: {e}")
            raise  # Raise the error for the caller to handle

    def upload_file(self, file_name, folder_id):
        """Upload a single file to a specific folder on Google Drive."""
        try:
            print(f"Uploading file: {file_name}")
            # Prepare file metadata
            file_metadata = {
                'name': os.path.basename(file_name),  # Use file name without path
                'parents': [folder_id]  # Set target folder
            }
            # Prepare the file content for upload
            media = MediaFileUpload(file_name, resumable=True)
            # Upload the file
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            print(f"File '{file_name}' uploaded with ID: {file.get('id')}")
            return file.get('id')
        except Exception as e:
            print(f"Error uploading file: {e}")
            raise

    def save_files(self, files):
        """Save multiple files to a Google Drive folder named after yesterday's date."""
        try:
            # Calculate yesterday's date in YYYY-MM-DD format
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            # Check if a folder for yesterday already exists
            folder_id = self.get_folder_id(yesterday)
            if not folder_id:
                # If not found, create it
                folder_id = self.create_folder(yesterday)
            
            # Upload each file to the folder
            for file_name in files:
                self.upload_file(file_name, folder_id)
            
            print(f"All files uploaded successfully to Google Drive folder '{yesterday}'.")
        except Exception as e:
            print(f"Error saving files: {e}")
            raise
