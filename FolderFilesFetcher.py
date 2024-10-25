import asyncio
from concurrent.futures import ThreadPoolExecutor
from googleapiclient.discovery import build

from RecursiveFolderFetcher import RecursiveFolderFetcher
from authenticate import authenticate


class FolderFilesFetcher:
    def __init__(self, folder_ids, max_concurrent_calls=20):
        self.folder_ids = folder_ids  # List of (folder_id, folder_name) tuples
        self.max_concurrent_calls = max_concurrent_calls  # Limit parallel API calls
        self.semaphore = asyncio.Semaphore(max_concurrent_calls)
        self.final_file_list = []  # Store tuples of (file_name, file_id, mime_type, folder_name)

        # Supported MIME types
        self.mime_types = [
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

    def create_service(self):
        # Authenticate or create a new service instance
        return build('drive', 'v3', credentials=authenticate())

    async def fetch_files_in_folder(self, folder_id, folder_name):
        """Asynchronously fetch files in a given folder ID, filtered by supported MIME types."""
        async with self.semaphore:
            service = await asyncio.get_event_loop().run_in_executor(None, self.create_service)
            page_token = None
            mime_types_query = " or ".join([f"mimeType='{mime_type}'" for mime_type in self.mime_types])
            query = f"({mime_types_query}) and '{folder_id}' in parents and trashed=false"

            while True:
                results = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: service.files().list(
                        pageSize=1000,
                        fields="nextPageToken, files(id, name, mimeType)",
                        pageToken=page_token,
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                        q=query
                    ).execute()
                )

                items = results.get('files', [])
                self.final_file_list.extend([(item['name'], item['id'], item['mimeType'], folder_name) for item in items])

                page_token = results.get('nextPageToken')
                if not page_token:
                    break

    async def fetch_all_files(self):
        """Fetch all files from each folder ID in parallel."""
        with ThreadPoolExecutor(max_workers=self.max_concurrent_calls) as executor:
            tasks = []
            for folder_id, folder_name in self.folder_ids:
                # Schedule fetching files in each folder
                tasks.append(asyncio.ensure_future(self.fetch_files_in_folder(folder_id, folder_name)))

            # Await all tasks to complete
            await asyncio.gather(*tasks)

# Example usage
if __name__ == '__main__':
    # Initialize RecursiveFolderFetcher to get all folder IDs
    folder_fetcher = RecursiveFolderFetcher(max_concurrent_calls=20)
    root_folder_id = '1VWELDrSkd1wAbR-L8sho4-eVQmNpxRx8'  # Replace with the actual folder ID if needed
    asyncio.run(folder_fetcher.fetch_all_folders(root_folder_id))

    # Initialize FolderFilesFetcher with folder IDs and names collected from RecursiveFolderFetcher
    file_fetcher = FolderFilesFetcher(folder_ids=folder_fetcher.all_folders, max_concurrent_calls=20)

    # Run asynchronous fetching of all files
    asyncio.run(file_fetcher.fetch_all_files())

    # Print all collected files
    print(f"Total files found: {len(file_fetcher.final_file_list)}")
    for file_name, file_id, mime_type, folder_name in file_fetcher.final_file_list:
        print(f"File Name: {file_name}, File ID: {file_id}, MIME Type: {mime_type}, Folder: {folder_name}")
