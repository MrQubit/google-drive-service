from authenticate import authenticate
from googleapiclient.discovery import build

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