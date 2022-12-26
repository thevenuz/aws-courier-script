import paramiko
from stat import S_ISDIR, S_ISREG
import json
import logging
from datetime import datetime
from pathlib import Path

class ImportFiles:
    def __init__(self) -> None:
        self.logger = self.logger_init()

    def logger_init(self):
        filename = f"log_{datetime.utcnow().strftime('%Y%m%d')}.txt"
        logging.basicConfig(filename=filename, format='%(asctime)s %(name)s %(levelname)s %(message)s', filemode='a')

        logger = logging.getLogger()
        logger.setLevel(logging.ERROR)

        return logger

    def load_settings(self):
        try:
            self.logger.info("load_settings method invoked")

            with open("settings.json", mode="r") as f:
                settings = json.load(f)

            return settings
        
        except Exception as e:
            self.logger.fatal("Exception occured in load_settings method", exc_info=1)
            raise e

    def trigger_import(self):
        try:
            self.logger.info("trigger_import method invoked")

            # load settings
            settings = self.load_settings()

            # set aws ssh details, local and remote paths of folders
            keypath = settings["KeyPath"]
            host = settings["Host"]
            port = settings["Port"]
            username = settings["Useranme"]
            localpath = settings["LocalPath"]
            remotepath = settings["RemotePath"]

            # load RSA privatekey path
            key = paramiko.RSAKey.from_private_key_file(keypath)

            transport = paramiko.Transport((host, port))

            transport.connect(username=username, pkey=key)

            sftp = paramiko.SFTPClient.from_transport(transport)

            # to create a folder based on date
            backupspath = self.build_local_path(localpath[0])

            result = self.copy_files(sftp, remotepath[0], backupspath)

            if not result:
                self.logger.fatal("Some error occured while copying files", exc_info=1)

            return result
        
        except Exception as e:
            self.logger.fatal("Exception occured in trigger_import method", exc_info=1)
            raise e
        

    def copy_files(self, sftp: paramiko.SFTPClient, fromPath: str, toPath: str) -> bool:
        try:
            self.logger.info("copy_files method invoked")

            for entry in sftp.listdir_attr(fromPath):
                mode = entry.st_mode

                if S_ISDIR(mode):
                    self.copy_files(sftp, f"{fromPath}/{entry.filename}", toPath)

                if S_ISREG(mode):
                    sftp.get(f"{fromPath}/{entry.filename}" , f"{toPath}/{entry.filename}")

            return True
        
        except Exception as e:
            self.logger.fatal("Exception occured in copy_files method", exc_info=1)
            return False
        
    def build_local_path(self, localpath: str) -> str:
        try:
            self.logger.info("build_local_path method invoked")

            today = datetime.today().strftime("%Y%m%d")

            filepath = f"{localpath}\\{today}"

            Path(filepath).mkdir(parents=True, exist_ok=True) 

            return filepath
        
        except Exception as e:
            self.logger.fatal("Exception occured in build_local_path method",exc_info=1)
            raise e
        

result = ImportFiles().trigger_import()

if result:
    print("succesfully copied files..")
else:
    print("Error while copying files")

