from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import io
from tqdm import tqdm

from googleapiclient.http import MediaIoBaseDownload

# Define the scopes that are requested from the Google Drive API
SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/drive.appdata",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive.meet.readonly",
    "https://www.googleapis.com/auth/drive.metadata",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
    "https://www.googleapis.com/auth/drive.photos.readonly",
    "https://www.googleapis.com/auth/drive.readonly"
]

SERVICE_ACCOUNT_FILE = './secrets/robust-summit-438914-g0-3220fe518938.json'
# Authenticate using the service account
def authenticate_service_account():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('drive', 'v3', credentials=creds)
    return service

def authenticate():
    creds = None
    # Check if token file already exists (to avoid re-authentication)
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If no valid credentials are available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                './secrets/my_secret.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for future runs
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds


def list_my_drive_files():
    creds = authenticate()
    service = build('drive', 'v3', credentials=creds)

    # Call the Drive API to list files only from "My Drive", excluding shared drives
    results = service.files().list(
        pageSize=1000,
        q="'me' in owners",  # Only list files owned by the authenticated user (in My Drive)
        fields="nextPageToken, files(id, name)",  # Specify the fields you want
        supportsAllDrives=False,  # Don't support shared drives
        includeItemsFromAllDrives=False  # Only include items from My Drive
    ).execute()

    items = results.get('files', [])

    if not items:
        print('No files found in My Drive.')
    else:
        print('Files in My Drive:')
        for item in items:
            print(f"{item['name']} ({item['id']})")


def get_file_by_id(file_id):
    creds = authenticate()
    service = build('drive', 'v3', credentials=creds)

    try:
        # Fetch the file metadata, supporting shared drives
        file = service.files().get(
            fileId=file_id,
            fields="id, name",
            supportsAllDrives=True
        ).execute()
        print(f"File found: {file['name']} ({file['id']})")
    except Exception as e:
        print(f"An error occurred: {e}")

def list_all_files(mime_types=None, folder_id=None, service=None):
    page_token = None
    all_files = []
    page_number = 0  # Initialize a counter for the pages
    total_files_fetched = 0

    # Default MIME types if not provided
    if mime_types is None:
        mime_types = [
            "text/plain",
            "application/vnd.google-apps.document",  # Google Docs
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",  # Word Document
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # Excel Spreadsheet
            "application/pdf",  # PDF
            "application/vnd.google-apps.presentation",  # Google Slides
            "application/vnd.openxmlformats-officedocument.presentationml.presentation",  # PowerPoint Presentation
            "application/vnd.google-apps.spreadsheet",  # Google Sheets
            "application/json"  # JSON
        ]

    # Constructing the query to filter by MIME types and folder
    mime_types_query = " or ".join([f"mimeType='{mime_type}'" for mime_type in mime_types])

    if folder_id:
        query = f"({mime_types_query}) and '{folder_id}' in parents and trashed=false"
    else:
        query = f"({mime_types_query}) and trashed=false"

    # Start fetching files page by page
    print("Fetching files page by page...")
    while True:
        page_number += 1  # Increment the page number
        print(f"Fetching page {page_number}...")  # Show progress for each page

        # Call the Drive API to list the files, supporting shared drives and including shared files
        results = service.files().list(
            pageSize=1000,
            fields="nextPageToken, files(id, name, mimeType)",
            pageToken=page_token,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            q=query
        ).execute()

        items = results.get('files', [])
        all_files.extend([(item['name'], item['id'], item['mimeType']) for item in items])
        total_files_fetched += len(items)

        print(f"Fetched {len(items)} files from page {page_number} (Total so far: {total_files_fetched})")

        page_token = results.get('nextPageToken')
        if not page_token:
            break

    print(f"Total files fetched: {total_files_fetched}")
    return all_files


def sanitize_filename(file_name):
    # This function sanitizes the file name to avoid issues with invalid characters.
    return "".join(c for c in file_name if c.isalnum() or c in (' ', '.', '_')).rstrip()

def download_file(file_name, file_id, mime_type, service, sync_folder):
    sanitized_file_name = sanitize_filename(file_name)  # Ensure file name is valid
    # Set the file extension based on the MIME type
    if mime_type == "application/vnd.google-apps.document":
        export_mime_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"  # docx
        extension = ".docx"
        is_google_file = True
    elif mime_type == "application/vnd.google-apps.spreadsheet":
        export_mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"  # xlsx
        extension = ".xlsx"
        is_google_file = True
    elif mime_type == "application/vnd.google-apps.presentation":
        export_mime_type = "application/vnd.openxmlformats-officedocument.presentationml.presentation"  # pptx
        extension = ".pptx"
        is_google_file = True
    elif mime_type == "application/pdf":
        extension = ".pdf"
        is_google_file = False
    elif mime_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        extension = ".docx"
        is_google_file = False
    elif mime_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        extension = ".xlsx"
        is_google_file = False
    elif mime_type == "application/vnd.openxmlformats-officedocument.presentationml.presentation":
        extension = ".pptx"
        is_google_file = False
    elif mime_type == "application/json":
        extension = ".json"
        is_google_file = False
    else:
        extension = ".txt"  # Default to text format for unknown MIME types
        is_google_file = False

    # Prepare the full file path
    file_path = os.path.join(sync_folder, f"{sanitized_file_name}.{file_id}{extension}")

    try:
        if is_google_file:
            # Google Docs Editors files (Docs, Sheets, Slides) need to be exported
            request = service.files().export_media(fileId=file_id, mimeType=export_mime_type)
        else:
            # Non-Google Docs files can be downloaded directly
            request = service.files().get_media(fileId=file_id)

        # Download the file
        fh = io.FileIO(file_path, mode='wb')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()

        print(f"Downloaded: {sanitized_file_name}.{file_id}{extension}")

    except Exception as e:
        error_message = str(e)
        if 'This file is too large to be exported.' in error_message:
            print(f"Can't download {sanitized_file_name}, file too large. Skipping file...")
        else:
            print(f"Unexpected Error: {error_message}")

if __name__ == '__main__':
    # Example usage
    # mime_types = [
    #     "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # Excel files
    #     "application/pdf"  # PDF files
    # ]
    creds = authenticate()
    service = build('drive', 'v3', credentials=creds)

    folder_id = "1XKRuVFEA2Fgfro1fuZ_kCe2lwFoERn9Q"  # Replace with actual folder ID or None
    list_all_files(folder_id=folder_id, service=service)

    file_list = list_all_files(service=service, folder_id=folder_id)

    # Download files to the sync folder
    sync_folder = './drive_sync'

    with tqdm(total=len(file_list), desc="Downloading files", unit="file") as progress_bar:
        for file_name, file_id, mime_type in file_list:
            download_file(file_name, file_id, mime_type, service, sync_folder)
            progress_bar.update(1)  # Update progress bar for each file