# Import necessary modules
import os  # For handling file and path operations
import json  # For working with JSON data
from google.oauth2.service_account import Credentials  # For authenticating using a service account
from googleapiclient.discovery import build  # For building the Google Drive API service
from googleapiclient.http import MediaFileUpload  # For uploading files to Google Drive
from datetime import datetime, timedelta  # For handling dates and times

# Define a class to handle saving files on Google Drive in the 'Furniture' category
class SavingOnDriveFurniture:
    def __init__(self, credentials_dict):
        # Store the provided credentials dictionary
        self.credentials_dict = credentials_dict

        # Define the required scope for accessing Google Drive
        self.scopes = ['https://www.googleapis.com/auth/drive']

        # Initialize the Drive API service variable (to be set after authentication)
        self.service = None

        # Define the parent folder ID where all files will be saved
        self.parent_folder_id = '1GOa6dAoHAvvycQaT8YYsbaj58CM-tVt5'  # Your parent folder ID

    def authenticate(self):
        """Authenticate with Google Drive API."""
        try:
            print("Authenticating with Google Drive...")

            # Create credentials using the provided service account info and scopes
            creds = Credentials.from_service_account_info(self.credentials_dict, scopes=self.scopes)

            # Build the Google Drive API service
            self.service = build('drive', 'v3', credentials=creds)

            print("Authentication successful.")
        except Exception as e:
            # Print and raise an error if authentication fails
            print(f"Authentication error: {e}")
            raise

    def get_folder_id(self, folder_name):
        """Get folder ID by name within the parent folder."""
        try:
            # Prepare query to find folder with the given name under the specified parent folder
            query = (f"name='{folder_name}' and "
                     f"'{self.parent_folder_id}' in parents and "
                     f"mimeType='application/vnd.google-apps.folder' and "
                     f"trashed=false")

            # Execute the query
            results = self.service.files().list(
                q=query,
                spaces='drive',
                fields='files(id, name)'
            ).execute()

            # Get the list of matching folders
            files = results.get('files', [])

            if files:
                # If folder is found, return its ID
                print(f"Folder '{folder_name}' found with ID: {files[0]['id']}")
                return files[0]['id']
            else:
                # If not found, return None
                print(f"Folder '{folder_name}' does not exist.")
                return None
        except Exception as e:
            # Handle and return any error encountered during the folder search
            print(f"Error getting folder ID: {e}")
            return None

    def create_folder(self, folder_name):
        """Create a new folder in the parent folder."""
        try:
            print(f"Creating folder '{folder_name}'...")

            # Prepare metadata for the new folder
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder',
                'parents': [self.parent_folder_id]
            }

            # Create the folder on Google Drive
            folder = self.service.files().create(
                body=file_metadata,
                fields='id'
            ).execute()

            # Return the newly created folder ID
            print(f"Folder '{folder_name}' created with ID: {folder.get('id')}")
            return folder.get('id')
        except Exception as e:
            # Print and raise any error encountered during folder creation
            print(f"Error creating folder: {e}")
            raise

    def upload_file(self, file_name, folder_id):
        """Upload a single file to Google Drive."""
        try:
            print(f"Uploading file: {file_name}")

            # Define metadata for the file to be uploaded
            file_metadata = {
                'name': os.path.basename(file_name),  # Use the file name only (no path)
                'parents': [folder_id]  # Place it under the specified folder
            }

            # Create a MediaFileUpload object for resumable upload
            media = MediaFileUpload(file_name, resumable=True)

            # Upload the file to Google Drive
            file = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()

            # Return the uploaded file's ID
            print(f"File '{file_name}' uploaded with ID: {file.get('id')}")
            return file.get('id')
        except Exception as e:
            # Handle and raise any error during file upload
            print(f"Error uploading file: {e}")
            raise

    def save_files(self, files):
        """Save files to Google Drive in a folder named after yesterday's date."""
        try:
            # Get yesterday's date in 'YYYY-MM-DD' format
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

            # Try to get the folder ID for yesterday's folder
            folder_id = self.get_folder_id(yesterday)

            # If folder doesn't exist, create a new one
            if not folder_id:
                folder_id = self.create_folder(yesterday)

            # Upload each file to the retrieved or created folder
            for file_name in files:
                self.upload_file(file_name, folder_id)

            print(f"All files uploaded successfully to Google Drive folder '{yesterday}'.")
        except Exception as e:
            # Handle and raise any error during the overall save process
            print(f"Error saving files: {e}")
            raise
