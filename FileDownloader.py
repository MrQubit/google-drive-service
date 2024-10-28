import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
import io
from tqdm import tqdm

from googleapiclient.http import MediaIoBaseDownload

from authenticate import create_service


class FileDownloader:
    def __init__(self, sync_folder):
        self.sync_folder = sync_folder

    def sanitize_filename(self, file_name):
        # This ensures that the file name is safe to use on most file systems
        return "".join(c for c in file_name if c.isalnum() or c in (' ', '.', '_')).rstrip()

    def get_extension_and_export_type(self, mime_type):
        # This function returns the correct extension and export MIME type based on the file type
        if mime_type == "application/vnd.google-apps.document":
            return ".docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        elif mime_type == "application/vnd.google-apps.spreadsheet":
            return ".xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        elif mime_type == "application/vnd.google-apps.presentation":
            return ".pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation"
        elif mime_type == "application/pdf":
            return ".pdf", None
        elif mime_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
            return ".docx", None
        elif mime_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
            return ".xlsx", None
        elif mime_type == "application/vnd.openxmlformats-officedocument.presentationml.presentation":
            return ".pptx", None
        elif mime_type == "application/json":
            return ".json", None
        else:
            # Default to .txt for unknown types, as a fallback
            return ".txt", None

    def download_file(self, file_name, file_id, mime_type, service):
        # Sanitize the file name
        sanitized_file_name = self.sanitize_filename(file_name)

        # Get the correct extension and export MIME type for the file
        extension, export_mime_type = self.get_extension_and_export_type(mime_type)

        # Construct the file path, ensure there's only one extension
        file_path = os.path.join(self.sync_folder, f"{sanitized_file_name}.{file_id}{extension}")

        try:
            # If it's a Google Docs-type file, export it using the correct MIME type
            if export_mime_type:
                request = service.files().export_media(fileId=file_id, mimeType=export_mime_type)
            else:
                # Otherwise, download it directly
                request = service.files().get_media(fileId=file_id)

            # Download the file in chunks
            with io.FileIO(file_path, mode='wb') as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk()

            print(f"Downloaded: {sanitized_file_name}.{file_id}{extension}")

        except Exception as e:
            if 'This file is too large to be exported.' in str(e):
                print(f"Cannot download {sanitized_file_name}, file too large. Skipping file...")
            else:
                print(f"Unexpected error: {e}")

    async def download_file_async(self, file_name, file_id, mime_type, semaphore):
        async with semaphore:
            # Create a new service instance in an async context
            service = await asyncio.get_event_loop().run_in_executor(None, create_service)
            # Call the download_file method in a thread pool executor
            await asyncio.get_event_loop().run_in_executor(
                None, self.download_file, file_name, file_id, mime_type, service
            )

    async def download_all_files_async(self, file_list):
        semaphore = asyncio.Semaphore(15)  # Limit concurrency to 15
        with ThreadPoolExecutor(max_workers=15) as executor:
            tasks = []

            # Set up progress bar
            with tqdm(total=len(file_list), desc="Downloading files", unit="file") as progress_bar:
                for file_item in file_list:
                    # Support both file_name, file_id, mime_type and file_name, file_id, mime_type, folder_name formats
                    if len(file_item) == 3:
                        file_name, file_id, mime_type = file_item
                    else:
                        file_name, file_id, mime_type, _ = file_item  # Ignore folder_name if present

                    # Schedule async download for each file
                    task = asyncio.ensure_future(
                        self.download_file_async(file_name, file_id, mime_type, semaphore)
                    )
                    tasks.append(task)
                    progress_bar.update(1)

                # Wait for all tasks to complete
                await asyncio.gather(*tasks)
