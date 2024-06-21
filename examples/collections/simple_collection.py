import argparse
import os
import shutil
import tempfile
import sys
from pathlib import Path
from exonfiledb.collection import Collection
from exonfiledb.defs import Buffer

DBPATH = Path(tempfile.gettempdir()) / "filedb"

def init(dbc):
    os.umask(0)
    if DBPATH.exists():
        shutil.rmtree(DBPATH)
    DBPATH.mkdir(parents=True, exist_ok=True)
    
    dbq = dbc.query()
    d = Buffer({"k1": [1, 2, 3]})
    for key in ["a.1.11", "a.1.12", "a.2.21", "b.1.11", "c.1.11","a.11"]:
        try:
            dbq.set_buffer(key, d)
        except Exception as e:
            print(f"error setting secure buffer for key {key}: {e}")
            sys.exit(1)
    print("Done")

def main():
    parser = argparse.ArgumentParser(description="FileDB")
    parser.add_argument("--init", action="store_true", help="initialize database store")
    args = parser.parse_args()
    
    print(f"\nUsing Database: {DBPATH}\n")

    dbc = Collection(DBPATH)
    
    if args.init:
        init(dbc)
        return
    
    col1 = dbc

    try:
        res = col1.query().keys()
        print(res, None)
    except Exception as e:
        print(e)
    
    try:
        res = col1.list_childs()
        print(res, None)
    except Exception as e:
        print(e)

    col2 = col1.child("a")
    print(f"Collection: {col2.base_path}")

    try:
        res = col2.query().keys()
        print(res, None)
    except Exception as e:
        print(e)

    try:
        res = col2.list_childs()
        print(res, None)
    except Exception as e:
        print(e)

    col3 = col2.child("2")
    print(f"Collection: {col3.base_path}", col3.is_exist())

    try:
        col1.copy("a.1", "c.1")
        print(None)
    except Exception as e:
        print(e)

    try:
        col1.copy("a", "c")
        print(None)
    except Exception as e:
        print(e)

    try:
        col1.move("c.a", "b")
        print(None)
    except Exception as e:
        print(e)

if __name__ == "__main__":
    main()
