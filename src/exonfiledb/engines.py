import os
import time
import errno
from threading import Event
from pathlib import Path
from exonfiledb.defs import (
    defaultOpTimeout,
    defaultOpPolling,
    defaultDirPerm,
    defaultFilePerm,
    TimeoutError,
    BreakError,
    ReadError,
    WriteError,
    LockedError
)

class FileEngine:
    # create new file engine
    def __init__(self):
        self.evt_break = Event()
        self.op_timeout = defaultOpTimeout
        self.op_polling = defaultOpPolling
        self.dir_perm = defaultDirPerm
        self.file_perm = defaultFilePerm

    # update file engine options
    def update_options(self, opts):
        self.op_timeout = opts.get("op_timeout", self.op_timeout)
        self.op_polling = opts.get("op_polling", self.op_polling)
        self.dir_perm = opts.get("dir_perm", self.dir_perm)
        self.file_perm = opts.get("file_perm", self.file_perm)
 
    # check if file exists and is regular file
    def file_exists(self, fpath):
        return Path(fpath).is_file()

    # read file content with shared locking
    def read_file(self, fpath):
        try:
            # open file for read
            with open(fpath, 'r') as f:
                # aquire file lock with retries
                self.acquire_file_lock(f, False, self.op_timeout, self.op_polling)
                data = f.read()
                self.release_file_lock(f)
                return data
        except Exception as e:
            raise ReadError(f"Error reading file: {e}")

    # write content to file with exclusive locking
    def write_file(self, fpath, data):
        try:
            # create dir tree for file if not exist
            if not self.file_exists(fpath):
                os.makedirs(os.path.dirname(fpath), exist_ok=True, mode=self.dir_perm)
            # open file for write
            with open(fpath, 'w') as f:
                # aquire file lock with retries
                self.acquire_file_lock(f, True, self.op_timeout, self.op_polling)
                f.write(data)
                self.release_file_lock(f)
        except Exception as e:
            raise WriteError(f"Error writing file: {e}")

    # create file if not exist
    def touch_file(self, fpath):
        try:
            # create dir tree for file if not exist
            if not self.file_exists(fpath):
                os.makedirs(os.path.dirname(fpath), exist_ok=True, mode=self.dir_perm)
            # open file for write
            with open(fpath, 'a'):
                os.utime(fpath, None)
        except Exception as e:
            raise WriteError(f"Error touching file: {e}")

    # delete file
    def purge_file(self, fpath):
        try:
            if os.path.isdir(fpath):
                os.rmdir(fpath)
            else:
                os.remove(fpath)
        except Exception as e:
            raise WriteError(f"Error deleting file: {e}")
    
    # cancel blocking operations 
    def cancel(self):
        self.evt_break.set()

    # aquire file lock with retries
    def acquire_file_lock(self, f, wr, tout, tpoll):
        self.evt_break.clear()
        tbreak = time.time() + tout
        while True:
            try:
                if wr:
                    # exclusive lock for writing
                    os.lockf(f.fileno(), os.F_LOCK, 0)
                else:
                    # shared lock for reading
                    os.lockf(f.fileno(), os.O_SHLOCK, 0)
                return
            except RuntimeError as e:
                if e.errno != errno.EAGAIN:
                    raise LockedError(f"Error acquiring file lock: {e}")
                if tout <= 0:
                    raise LockedError("File is locked and timeout is set to zero")
                time.sleep(tpoll)
                if self.evt_break.is_set():
                    raise BreakError("Operation cancelled")
                if time.time() >= tbreak:
                    raise TimeoutError("Operation timed out")
   
    # release file lock
    def release_file_lock(self, f):
        os.lockf(f.fileno(), os.F_ULOCK, 0)
