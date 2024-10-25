import asyncio
import re
from concurrent.futures import ThreadPoolExecutor
from googleapiclient.discovery import build
import os
import io
from tqdm import tqdm

from googleapiclient.http import MediaIoBaseDownload

from RecusiveFileFetch import RecursiveFileFetch
from authenticate import authenticate


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


def filter_files(file_list):
    """
    Filters the given list of files by removing files that have the same base name,
    differing only by numbers or identifier strings.

    Parameters:
    - file_list: List of tuples [(name, id, mimeType), ...]

    Returns:
    - filtered_files: List of tuples that passed the filter
    """
    def get_base_name(file_name):
        # Split the file name into tokens using non-alphanumeric characters
        tokens = re.split(r'\W+', file_name)
        # Remove tokens that are entirely numeric
        # Remove tokens that are length <=2 and contain digits
        tokens = [
            token for token in tokens
            if not (
                token.isdigit() or
                (len(token) <= 2 and any(char.isdigit() for char in token))
            )
        ]
        # Join the remaining tokens and convert to lowercase
        base_name = ''.join(tokens).lower()
        return base_name

    # Build a mapping from base names to lists of files
    base_name_to_files = {}
    for item in file_list:
        name, file_id, mime_type = item
        base_name = get_base_name(name)
        if base_name in base_name_to_files:
            base_name_to_files[base_name].append(item)
        else:
            base_name_to_files[base_name] = [item]

    # Filter the list: remove files that have the same base name as others
    filtered_files = []
    for base_name, files in base_name_to_files.items():
        if len(files) < 5:
            # Keep files with unique base names
            filtered_files.extend(files)
        else:
            # Remove files with duplicate base names
            pass  # Do nothing, as we are removing these files

    return filtered_files

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
    filter_files_list= filter_files(all_files)
    # Once all files are fetched, write them to the output file at the end
    output_file = 'all_files.txt'
    with open(output_file, 'w', encoding='utf-8') as f:  # Specify utf-8 encoding
        for file_name, file_id, mime_type in filter_files_list:
            f.write(f"{file_name}, {file_id}, {mime_type}\n")

    print(f"Total files fetched: {total_files_fetched}")
    return filter_files_list


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


# Create a separate service instance for each task
def create_service():
    credentials = authenticate()  # This function should return credentials
    return build('drive', 'v3', credentials=credentials)


async def download_file_async(file_name, file_id, mime_type, sync_folder, semaphore):
    async with semaphore:
        # Create a new service instance for each download
        service = await asyncio.get_event_loop().run_in_executor(None, create_service)

        # Call the download_file function with the new service instance
        await asyncio.get_event_loop().run_in_executor(None, download_file, file_name, file_id, mime_type, service,
                                                       sync_folder)


async def download_all_files_async(file_list, sync_folder):
    semaphore = asyncio.Semaphore(15)  # Limit to 3 concurrent downloads
    with ThreadPoolExecutor(max_workers=15) as executor:
        tasks = []

        # Set up a progress bar
        with tqdm(total=len(file_list), desc="Downloading files", unit="file") as progress_bar:
            for file_name, file_id, mime_type in file_list:
                # Schedule each download asynchronously
                task = asyncio.ensure_future(
                    download_file_async(file_name, file_id, mime_type, sync_folder, semaphore)
                )
                tasks.append(task)

                # Update the progress bar after each file is scheduled for download
                progress_bar.update(1)

            # Wait for all downloads to complete
            await asyncio.gather(*tasks)

if __name__ == '__main__':
    # Example usage
    # mime_types = [
    #     "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # Excel files
    #     "application/pdf"  # PDF files
    # ]
    creds = authenticate()
    service = build('drive', 'v3', credentials=creds)


    # folder_id = "1XKRuVFEA2Fgfro1fuZ_kCe2lwFoERn9Q"  # Replace with actual folder ID or None?
    # folder_id = "1V7Tva6lz-ugsKvAZ9ztRt6Hh9eTsqakz" # testing folder with 22 files
    folder_id = "1VWELDrSkd1wAbR-L8sho4-eVQmNpxRx8" # Kinit guidelines


    # file_list = list_all_files(service=service, folder_id="1V7Tva6lz-ugsKvAZ9ztRt6Hh9eTsqakz")

    # excluded_folders = ['folderId1', 'folderId2']  # Replace with actual folder IDs

    # Initialize RecursiveFileFetch class

    # Decide to use recursive or other fetch approach
    use_recursive = True  # Set this to False if you want to use a different approach
    file_fetcher = RecursiveFileFetch(service=service, excluded_folders=None)
    if use_recursive:
        # Recursive fetch of all files
        file_list = file_fetcher.list_all_files(folder_id=folder_id, output_file='all_files.txt')



    # Download files to the sync folder
    # sync_folder = './drive_sync'
    #
    # # Run asynchronous download of all files
    # start =  time.time()
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(download_all_files_async(file_list, sync_folder))
    # print("Download time: ", time.time() - start)