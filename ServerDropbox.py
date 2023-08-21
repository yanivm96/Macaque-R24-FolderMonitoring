import requests 
import dropbox
from dropbox.exceptions import AuthError
import os


class ServerDropbox():
    def __init__(self):
        self.DROPBOX_ACCESS_TOKEN = None
        self.REFRESH_TOKEN = ''
        self.APP_KEY = ''
        self.APP_SECRET = ''
        self.connected_dropbox = None

    
    def refresh_access_token(self):
        token_url = 'https://api.dropboxapi.com/oauth2/token'
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': self.REFRESH_TOKEN,
            'client_id': self.APP_KEY,
            'client_secret': self.APP_SECRET
        }
        
        response = requests.post(token_url, data=data)
        if response.status_code == 200:
            token_data = response.json()
            self.DROPBOX_ACCESS_TOKEN = token_data['access_token']
            self.REFRESH_TOCKEN = token_data.get('refresh_token', self.REFRESH_TOKEN)
            
        else:
            raise Exception("Failed to refresh access token")
        
    def connect_to_dropbox(self):
        self.refresh_access_token()
        self.connected_dropbox = dropbox.Dropbox(self.DROPBOX_ACCESS_TOKEN)


    def get_files_without_cursor(self, folder_path):
        return self.connected_dropbox.files_list_folder(folder_path)

    def get_files_with_cursor(self, cursor):
        return self.connected_dropbox.files_list_folder_continue(cursor)

    def generate_refresh_token(self): #should run only once, refresh token is not expired
        AUTHORIZATION_CODE = ''       #should get it from the url:https://www.dropbox.com/oauth2/authorize?client_id=<APP_KEY>&token_access_type=offline&response_type=code 
                                      #should fill your appkey in the url

        token_url = 'https://api.dropboxapi.com/oauth2/token'
        data = {
            'code': AUTHORIZATION_CODE,
            'grant_type': 'authorization_code'
        }
        auth = (self.APP_KEY, self.APP_SECRET)
        response = requests.post(token_url, data=data, auth=auth)

        if response.status_code == 200:
            token_data = response.json()
            self.DROPBOX_ACCESS_TOKEN = token_data['access_token']
            self.REFRESH_TOKEN = token_data['refresh_token']
        else:
            print("Token request failed:", response.status_code, response.text)
            exit(1)
        print("Access Token:", self.DROPBOX_ACCESS_TOKEN)
        print("Refresh Token:", self.REFRESH_TOKEN)

    
    def download_entry(self,entry, local_path):
        try:
            if isinstance(entry, dropbox.files.FileMetadata):
                with open(local_path, 'wb') as f:
                    metadata, res = self.connected_dropbox.files_download(entry.path_display)
                    f.write(res.content)
            elif isinstance(entry, dropbox.files.FolderMetadata):
                # For folders, create a corresponding local folder
                os.makedirs(local_path, exist_ok=True)
                # Download the contents of the folder recursively
                self.download_folder_contents(entry.path_display, local_path)
                
        except dropbox.exceptions.HttpError as e:
            print(f"Error downloading {entry.path_display}: {e}")

    def download_folder_contents(self, folder_path, local_path):
        # List the contents of the folder
        result = self.connected_dropbox.files_list_folder(folder_path)
        for entry in result.entries:
            entry_local_path = os.path.join(local_path, entry.name)
            self.download_entry(entry, entry_local_path)



    
