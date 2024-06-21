import json
import os
from exonfiledb.engines import FileEngine
from exonfiledb.defs import *

class Query:
    def __init__(self, collection):
        self.file_engine = FileEngine()
        self.collection = collection

    def keys(self):
        res = []
        try:
            for root, dirs, files in os.walk(self.collection.base_path):
                for file in files:
                    if not file.endswith(keyBakSuffix):
                        res.append(os.path.relpath(os.path.join(root, file), self.collection.base_path))
                dirs.clear()
        except Exception as e:
            raise e
        return res

    def is_exist(self, key):
        return self.file_engine.file_exists(self.collection.key_path(key))

    def get(self, key):
        keypath = self.collection.key_path(key)
        keybakpath = self.collection.key_path(key + keyBakSuffix)
        err = NotExistError

        # check main file
        if self.file_engine.file_exists(keypath):
            try:
                data = self.file_engine.read_file(keypath)
                self.file_engine.write_file(keybakpath, data)
                return data
            except Exception as e:
                err = e

        # check backup
        if self.file_engine.file_exists(keybakpath):
            try:
                data = self.file_engine.read_file(keybakpath)
                self.file_engine.write_file(keypath, data)
                return data
            except Exception as e:
                err = e

        raise err

    def get_buffer(self, key):
        keypath = self.collection.key_path(key)
        keybakpath = self.collection.key_path(key + keyBakSuffix)
        err = NotExistError

        if self.file_engine.file_exists(keypath):
            try:
                rawdata = self.file_engine.read_file(keypath)
                data = json.loads(rawdata)
                self.file_engine.write_file(keybakpath, rawdata)
                return Buffer(data)
            except Exception as e:
                err = e

        if self.file_engine.file_exists(keybakpath):
            try:
                rawdata = self.file_engine.read_file(keybakpath)
                data = json.loads(rawdata)
                self.file_engine.write_file(keypath, rawdata)
                return Buffer(data)
            except Exception as e:
                err = e

        raise err

    def get_buffer_slice(self, key):
        keypath = self.collection.key_path(key)
        keybakpath = self.collection.key_path(key + keyBakSuffix)
        err = NotExistError

        if self.file_engine.file_exists(keypath):
            try:
                rawdata = self.file_engine.read_file(keypath)
                data = json.loads(rawdata)
                self.file_engine.write_file(keybakpath, rawdata)
                return [Buffer(d) for d in data]
            except Exception as e:
                err = e

        if self.file_engine.file_exists(keybakpath):
            try:
                rawdata = self.file_engine.read_file(keybakpath)
                data = json.loads(rawdata)
                self.file_engine.write_file(keypath, rawdata)
                return [Buffer(d) for d in data]
            except Exception as e:
                err = e

        raise err

    def set(self, key, value):
        keypath = self.collection.key_path(key)
        keybakpath = self.collection.key_path(key + keyBakSuffix)

        try:
            self.file_engine.write_file(keypath, value)
            self.file_engine.write_file(keybakpath, value)
        except Exception as e:
            raise e

    def set_buffer(self, key, value):
        try:
            data = json.dumps(value, indent=2)
            self.set(key, data)
        except Exception as e:
            raise WriteError(f"{WriteError} - {e}")

    def set_buffer_slice(self, key, value):
        try:
            data = json.dumps(value, indent=2)
            self.set(key, data)
        except Exception as e:
            raise WriteError(f"{WriteError} - {e}")

    def delete(self, key):
        keypath = self.collection.key_path(key)
        keybakpath = self.collection.key_path(key + keyBakSuffix)

        if self.file_engine.file_exists(keybakpath):
            self.file_engine.purge_file(keybakpath)
        if self.file_engine.file_exists(keypath):
            self.file_engine.purge_file(keypath)

    def get_secure(self, key):
        if self.collection.cipher is None:
            raise NoSecurityError("no cipher available for decryption")

        keypath = self.collection.key_path(key)
        keybakpath = self.collection.key_path(key + keyBakSuffix)

        try:
            if self.file_engine.file_exists(keypath):
                rawdata = self.file_engine.read_file(keypath)
                value = self.collection.cipher.decrypt(rawdata)
                self.file_engine.write_file(keybakpath, rawdata)
                return value
        except Exception as err:
            pass

        try:
            if self.file_engine.file_exists(keybakpath):
                rawdata = self.file_engine.read_file(keybakpath)
                value = self.collection.cipher.decrypt(rawdata)
                self.file_engine.write_file(keypath, rawdata)
                return value
        except Exception as err:
            pass

        raise NotExistError(f"Key {key} does not exist")

    def get_secure_buffer(self, key):
        if self.collection.cipher is None:
            raise NoSecurityError("no cipher available for decryption")

        keypath = self.collection.key_path(key)
        keybakpath = self.collection.key_path(key + keyBakSuffix)

        try:
            if self.file_engine.file_exists(keypath):
                rawdata = self.file_engine.read_file(keypath)
                value = self.collection.cipher.decrypt(rawdata)
                data = json.loads(value)
                self.file_engine.write_file(keybakpath, rawdata)
                return Buffer(data)
        except Exception as err:
            pass

        try:
            if self.file_engine.file_exists(keybakpath):
                rawdata = self.file_engine.read_file(keybakpath)
                value = self.collection.cipher.decrypt(rawdata)
                data = json.loads(value)
                self.file_engine.write_file(keypath, rawdata)
                return Buffer(data)
        except Exception as err:
            pass

        raise NotExistError(f"Key {key} does not exist")

    def get_secure_buffer_slice(self, key):
        if self.collection.cipher is None:
            raise NoSecurityError("no cipher available for decryption")

        keypath = self.collection.key_path(key)
        keybakpath = self.collection.key_path(key + keyBakSuffix)

        try:
            if self.file_engine.file_exists(keypath):
                rawdata = self.file_engine.read_file(keypath)
                value = self.collection.cipher.decrypt(rawdata)
                data = json.loads(value)
                self.file_engine.write_file(keybakpath, rawdata)
                return [Buffer(d) for d in data]
        except Exception as err:
            pass

        try:
            if self.file_engine.file_exists(keybakpath):
                rawdata = self.file_engine.read_file(keybakpath)
                value = self.collection.cipher.decrypt(rawdata)
                data = json.loads(value)
                self.file_engine.write_file(keypath, rawdata)
                return [Buffer(d) for d in data]
        except Exception as err:
            pass

        raise NotExistError(f"key {key} does not exist")

    # write content to file with exclusive locking
    def set_secure(self, key, value):
        if self.collection.cipher is None:
            raise NoSecurityError("no cipher available for encryption")

        try:
            encrypted_data = self.collection.cipher.encrypt(value)
            self.set(key, encrypted_data)
        except Exception as err:
            raise EncryptError(f"error encrypting key {key}: {err}")

    def set_secure_buffer(self, key, value):
        data = json.dumps(value)
        self.set_secure(key, data)

    def set_secure_buffer_slice(self, key, value):
        data = json.dumps([dict(v) for v in value])
        self.set_secure(key, data)
