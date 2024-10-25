from authenticate import authenticate


class RecursiveFileFetch:
    def __init__(self, service, mime_types=None, excluded_folders=None):
        self.service = service
        self.mime_types = mime_types if mime_types else [
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
        self.excluded_folders = excluded_folders if excluded_folders else []

    def list_files_in_folder(self, folder_id):
        """Fetch files recursively from a folder and its subfolders, excluding specified folders."""
        all_files = []
        page_token = None
        mime_types_query = " or ".join([f"mimeType='{mime_type}'" for mime_type in self.mime_types])

        # Query to fetch files (not folders) from the current folder
        query = f"({mime_types_query}) and '{folder_id}' in parents and trashed=false"

        # Fetch files from the current folder
        while True:
            results = self.service.files().list(
                pageSize=1000,
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                q=query
            ).execute()

            items = results.get('files', [])
            all_files.extend([(item['name'], item['id'], item['mimeType']) for item in items])

            page_token = results.get('nextPageToken')
            if not page_token:
                break

        # Now, list all subfolders and recursively fetch files from them
        subfolders_query = f"mimeType='application/vnd.google-apps.folder' and '{folder_id}' in parents and trashed=false"
        page_token = None
        subfolders = []

        while True:
            results = self.service.files().list(
                pageSize=1000,
                fields="nextPageToken, files(id, name)",
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                q=subfolders_query
            ).execute()

            items = results.get('files', [])
            subfolders.extend(items)

            page_token = results.get('nextPageToken')
            if not page_token:
                break

        # Recursively fetch files from subfolders, excluding specified folders
        for subfolder in subfolders:
            if subfolder['id'] not in self.excluded_folders:
                print(f"Processing subfolder: {subfolder['name']}")
                subfolder_files = self.list_files_in_folder(subfolder['id'])
                all_files.extend(subfolder_files)
            else:
                print(f"Skipping excluded folder: {subfolder['name']}")

        return all_files

    def list_all_files(self, folder_id='root', output_file='all_files.txt'):
        """Fetch all files recursively from the given folder and save to a file, excluding specific folders."""
        print(f"Fetching files from folder: {folder_id} and its subfolders, excluding: {self.excluded_folders}")
        all_files = self.list_files_in_folder(folder_id)

        # Once all files are fetched, write them to the output file with utf-8 encoding
        with open(output_file, 'w', encoding='utf-8') as f:
            for file_name, file_id, mime_type in all_files:
                f.write(f"{file_name}, {file_id}, {mime_type}\n")

        print(f"Total files fetched: {len(all_files)}")
        return all_files


# Example usage:
if __name__ == '__main__':
    # Authenticate and initialize the service
    service = build('drive', 'v3', credentials=authenticate())

    # Define the folder IDs you want to exclude
    excluded_folders = ['folderId1', 'folderId2']  # Replace with actual folder IDs

    # Initialize RecursiveFileFetch class
    file_fetcher = RecursiveFileFetch(service=service, excluded_folders=excluded_folders)

    # Decide to use recursive or other fetch approach
    use_recursive = True  # Set this to False if you want to use a different approach

    if use_recursive:
        # Recursive fetch of all files
        file_list = file_fetcher.list_all_files(folder_id='root', output_file='all_files.txt')
    else:
        # Placeholder for a different fetch approach, if implemented
        print("Using non-recursive approach (yet to be implemented).")
