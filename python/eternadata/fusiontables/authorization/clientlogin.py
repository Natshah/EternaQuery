import getpass
import requests


class ClientLogin():

    def __init__(self, username=None, password=None):
        if username is None:
            username = raw_input("Enter your Google username: ")    
        if password is None:
            password = getpass.getpass("Enter your password: ")
        self._authdata = {
            'Email': username,
            'Passwd': password,
            'service': 'fusiontables',
            'accountType': 'HOSTED_OR_GOOGLE'
        }
        self.token = None

    def authorize(self):
        url = 'https://www.google.com/accounts/ClientLogin'
        resp = requests.post(url, json=self._authdata)
        self.token = resp.json()['Auth']
        return self.token

