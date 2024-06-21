import shutil
from exonfiledb.defs import keySep,ReadError

class Index:
    def __init__(self, collection):
        self.collection = collection

    def list(self):
        return self.collection.query().keys()

    def list_indexes(self):
        return self.collection.list_childs()

    def check(self, key):
        return self.collection.query().is_exist(key)

    def mark(self, key):
        fpath = self.collection.key_path(key)
        return self.collection.query().file_engine.touch_file(fpath)

    def clear(self, key):
        return self.collection.query().delete(key)

    def clear_all(self, key):
        indxlist, err = self.list_indexes()
        if err:
            return err
        for ix in indxlist:
            self.collection.query().delete(ix + keySep + key)
        return None

    def purge(self):
        try:
            shutil.rmtree(self.collection.base_path)
        except Exception as e:
            raise ReadError(f"error purging index: {e}")