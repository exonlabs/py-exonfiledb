import os

keySep = "."
keyBakSuffix = "_bak"
fileSep = os.path.sep
defaultOpTimeout = 3.0
defaultOpPolling = 0.1
defaultDirPerm = 0o775
defaultFilePerm = 0o664

class Options(dict):
    def get(self, key, default=None):
        return super().get(key, default)

class Buffer(dict):
    pass

class CustomError(Exception):
    pass

class TimeoutError(CustomError):
    pass

class BreakError(CustomError):
    pass

class ReadError(CustomError):
    pass

class WriteError(CustomError):
    pass

class NotExistError(CustomError):
    pass

class LockedError(CustomError):
    pass

class NoSecurityError(CustomError):
    pass

class InvalidKeyError(CustomError):
    pass

class EncryptError(CustomError):
    pass

class DecryptError(CustomError):
    pass

class FileEngineError(CustomError):
    pass