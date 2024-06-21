import json
import os
import shutil
from pathlib import Path
from exonfiledb.engines import FileEngine
from exonfiledb.query import Query
from exonfiledb.index import Index  
from exonfiledb.defs import *

class Collection:
    def __init__(self, base_path):
        self.base_path = Path(base_path)
        self.file_engine = FileEngine()
        self.base_path.mkdir(parents=True, exist_ok=True)

    # convert relative file or collection key to absolute path
    def key_path(self, key):
        return self.base_path / key.replace(keySep, os.sep)

    def is_exist(self):
        return self.base_path.exists()

    def copy(self, src, dest):
        try:
            src_path = self.key_path(src)
            dest_path = self.key_path(dest)
            if not src_path.exists():
                raise FileNotFoundError(f"source path does not exist: {src_path}")
            dest_path = dest_path / src_path.name
            if dest_path.exists():
                shutil.rmtree(dest_path)
            if src_path.is_dir():
                shutil.copytree(src_path, dest_path)
            else:
                shutil.copy2(src_path, dest_path)
        except Exception as e:
            raise WriteError(f"error copying from {src} to {dest}: {e}")
 
    def purge(self, key):
        if not key:
            raise ValueError("key is not defined")

        keypath = self.key_path(key)

        try:
            if not self.file_engine.file_exists(keypath):
                return None 

            # Check if it's a directory and not empty
            if os.path.isdir(keypath) and os.listdir(keypath):
                raise FileEngineError("key is not collection")

            # Remove file or directory
            self.file_engine.purge_file(keypath)
        except Exception as e:
            raise FileEngineError(e)

    def move(self, src, dest):
        try:
            src_path = self.key_path(src)
            dest_path = self.key_path(dest)
            if not src_path.exists():
                raise FileNotFoundError(f"source path does not exist: {src_path}")
            dest_path = dest_path / src_path.name
            if dest_path.exists():
                shutil.rmtree(dest_path)
            shutil.move(src_path, dest_path)
        except Exception as e:
            raise WriteError(f"error moving from {src} to {dest}: {e}")

########################### child methods
        
    # create child collection relative to parent collection
    def child(self, name):
        return Collection(self.key_path(name))
   
    def list_childs(self):
        try:
            return [
                d for d in os.listdir(self.base_path)
                if os.path.isdir(os.path.join(self.base_path, d)) and not d.startswith('.')
            ]
        except Exception as e:
            raise ReadError(f"error listing child collections: {e}")
        
    def list_indexes(self):
        try:
            return [
                d[len('.ix_'):] for d in os.listdir(self.base_path)
                if os.path.isdir(os.path.join(self.base_path, d)) and d.startswith('.ix_')
            ]
        except Exception as e:
            raise ReadError(f"error listing indexes: {e}")

    def keys(self):
        res = []
        try:
            for root, dirs, files in os.walk(self.base_path):
                for file in files:
                    if not file.endswith(keyBakSuffix):
                        res.append(os.path.relpath(os.path.join(root, file), self.base_path))
                dirs.clear()
        except Exception as e:
            raise e
        return res

########################### Query methods

    def query(self):
        return Query(self)

    def index(self,key):
        parts= key.split(".")
        parts[0]=".ix_"+ parts[0]
        
        path = os.path.join(self.base_path, *parts)
        col = Collection(path)
        return Index(collection=col)