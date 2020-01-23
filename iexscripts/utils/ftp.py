import os
from ftplib import FTP
from iexscripts.constants import (FTP_HOST,
                                  FTP_PORT,
                                  FTP_USER,
                                  FTP_PASSWORD)

class Ftp:
    def __init__(self):
        """
        Return FTP object
        """

        self._ftp = FTP()
        self._ftp.connect(host=FTP_HOST, port=FTP_PORT)
        self._ftp.login(user=FTP_USER, passwd=FTP_PASSWORD)
        self._ftp.cwd('data')

    @property
    def ftp(self):
        return self._ftp

    # Upload files
    def placeFiles(self, dirpath, file_list):
        for filename in file_list:
            self.ftp.storbinary('STOR '+filename, open(os.path.join(dirpath,filename), 'rb'))

    def quitConnection(self):
        self.ftp.quit()
