from FileDownloader import FileDownloader
from FolderFilesFetcher import FolderFilesFetcher
from RecursiveFolderFetcher import RecursiveFolderFetcher
import asyncio

async def main():
    try:
        # Initialize RecursiveFolderFetcher to get all folder IDs
        folder_fetcher = RecursiveFolderFetcher(max_concurrent_calls=20)
        root_folder_id = '1VWELDrSkd1wAbR-L8sho4-eVQmNpxRx8'  # Kinit guidelines
        await folder_fetcher.fetch_all_folders(root_folder_id)

        # Initialize FolderFilesFetcher with folder IDs and names collected from RecursiveFolderFetcher
        file_fetcher = FolderFilesFetcher(folder_ids=folder_fetcher.all_folders, max_concurrent_calls=20)

        # Run asynchronous fetching of all files
        await file_fetcher.fetch_all_files()

        # Print all collected files
        print(f"Total files found: {len(file_fetcher.final_file_list)}")
        for file_name, file_id, mime_type, folder_name in file_fetcher.final_file_list:
            print(f"File Name: {file_name}, File ID: {file_id}, MIME Type: {mime_type}, Folder: {folder_name}")

        # File downloading
        sync_folder = './guidelines'
        file_downloader = FileDownloader(sync_folder=sync_folder)

        # Run asynchronous download of all files
        await file_downloader.download_all_files_async(file_fetcher.final_file_list)

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == '__main__':
    asyncio.run(main())
