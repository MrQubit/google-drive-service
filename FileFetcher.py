import re


class FileFetcher:
    def __init__(self, service):
        self.service = service

    def filter_files(self, file_list):
        def get_base_name(file_name):
            tokens = re.split(r'\W+', file_name)
            tokens = [
                token for token in tokens
                if not (
                    token.isdigit() or
                    (len(token) <= 2 and any(char.isdigit() for char in token))
                )
            ]
            base_name = ''.join(tokens).lower()
            return base_name

        base_name_to_files = {}
        for item in file_list:
            name, file_id, mime_type = item
            base_name = get_base_name(name)
            if base_name in base_name_to_files:
                base_name_to_files[base_name].append(item)
            else:
                base_name_to_files[base_name] = [item]

        filtered_files = []
        for base_name, files in base_name_to_files.items():
            if len(files) < 5:
                filtered_files.extend(files)

        return filtered_files

    def list_all_files(self, mime_types=None, folder_id=None):
        page_token = None
        all_files = []
        page_number = 0
        total_files_fetched = 0

        if mime_types is None:
            mime_types = [
                "text/plain",
                "application/vnd.google-apps.document",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "application/pdf",
                "application/vnd.google-apps.presentation",
                "application/vnd.openxmlformats-officedocument.presentationml.presentation",
                "application/vnd.google-apps.spreadsheet",
                "application/json"
            ]

        mime_types_query = " or ".join([f"mimeType='{mime_type}'" for mime_type in mime_types])

        if folder_id:
            query = f"({mime_types_query}) and '{folder_id}' in parents and trashed=false"
        else:
            query = f"({mime_types_query}) and trashed=false"

        print("Fetching files page by page...")
        while True:
            page_number += 1
            print(f"Fetching page {page_number}...")

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
            total_files_fetched += len(items)

            print(f"Fetched {len(items)} files from page {page_number} (Total so far: {total_files_fetched})")

            page_token = results.get('nextPageToken')
            if not page_token:
                break

        print(f"Total files fetched: {total_files_fetched}")
        filtered_files_list = self.filter_files(all_files)

        output_file = 'all_files.txt'
        with open(output_file, 'w', encoding='utf-8') as f:
            for file_name, file_id, mime_type in filtered_files_list:
                f.write(f"{file_name}, {file_id}, {mime_type}\n")

        return filtered_files_list