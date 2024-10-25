import asyncio
from concurrent.futures import ThreadPoolExecutor
from googleapiclient.discovery import build

from authenticate import authenticate


class RecursiveFolderFetcher:
    def __init__(self, max_concurrent_calls=20):
        self.max_concurrent_calls = max_concurrent_calls  # Limit parallel API calls
        self.semaphore = asyncio.Semaphore(max_concurrent_calls)
        self.all_folders = []  # Store tuples of (id, name)

    def create_service(self):
        # Authenticate or create a new service instance
        return build('drive', 'v3', credentials=authenticate())

    async def fetch_subfolders(self, folder_id):
        """Asynchronously fetch subfolders for a given folder ID."""
        async with self.semaphore:
            service = await asyncio.get_event_loop().run_in_executor(None, self.create_service)
            subfolders = []
            page_token = None

            while True:
                query = f"mimeType='application/vnd.google-apps.folder' and '{folder_id}' in parents and trashed=false"
                results = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: service.files().list(
                        pageSize=1000,
                        fields="nextPageToken, files(id, name)",
                        pageToken=page_token,
                        supportsAllDrives=True,
                        includeItemsFromAllDrives=True,
                        q=query
                    ).execute()
                )

                items = results.get('files', [])
                subfolders.extend([(item['id'], item['name']) for item in items])

                page_token = results.get('nextPageToken')
                if not page_token:
                    break

            return subfolders

    async def fetch_all_folders(self, root_folder_id):
        """Fetch all subfolder IDs and names under the given folder using parallel calls."""
        to_process = [(root_folder_id, "Root Folder")]  # Start with the root folder
        all_folders = set()

        with ThreadPoolExecutor(max_workers=self.max_concurrent_calls) as executor:
            while to_process:
                current_level = to_process
                to_process = []

                tasks = []
                for folder_id, folder_name in current_level:
                    # Schedule fetching subfolders for each folder in the current level
                    tasks.append(asyncio.ensure_future(self.fetch_subfolders(folder_id)))

                # Await all tasks for the current level
                results = await asyncio.gather(*tasks)

                for subfolder_list in results:
                    for folder_id, folder_name in subfolder_list:
                        if folder_id not in all_folders:
                            all_folders.add(folder_id)
                            to_process.append((folder_id, folder_name))
                            self.all_folders.append((folder_id, folder_name))


# Example usage
if __name__ == '__main__':
    # Initialize RecursiveFolderFetcher
    folder_fetcher = RecursiveFolderFetcher(max_concurrent_calls=20)

    # Run asynchronous folder fetching for a specific folder ID
    root_folder_id = '1VWELDrSkd1wAbR-L8sho4-eVQmNpxRx8'  # Replace with the actual folder ID if needed
    asyncio.run(folder_fetcher.fetch_all_folders(root_folder_id))

    # Print all collected folder IDs and names
    print(f"Total folders found: {len(folder_fetcher.all_folders)}")
    for folder_id, folder_name in folder_fetcher.all_folders:
        print(f"Folder ID: {folder_id}, Folder Name: {folder_name}")
