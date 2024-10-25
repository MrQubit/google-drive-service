
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os

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