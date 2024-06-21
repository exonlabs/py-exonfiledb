import argparse
import sys
import os
import shutil
import tempfile
from exonfiledb.collection import Collection
from exonfiledb.defs import Buffer


DBPATH = os.path.join(tempfile.gettempdir(), "filedb")

def main():
    parser = argparse.ArgumentParser(description="Database initialization")
    parser.add_argument("--init", action="store_true", help="initialize database store")
    args = parser.parse_args()

    print(f"\nUsing Database: {DBPATH}")

    dbc = Collection(DBPATH)

    if args.init:
        os.umask(0)
        shutil.rmtree(DBPATH, ignore_errors=True)
        os.makedirs(DBPATH, mode=0o777, exist_ok=True)

        dbq = dbc.query()
        d = Buffer({"k1": [1, 2, 3]})
        for key in ["a.1.11", "a.1.12", "a.2.21", "b.1.11", "c.1.11","a.11"]:
            try:
                dbq.set_buffer(key, d)
            except Exception as e:
                print(f"error setting secure buffer for key {key}: {e}")
                sys.exit(1)

        idx = dbc.index("index.key1")
        idx.mark("v1")
        idx.mark("v2")
        dbc.index("index.key2").mark("v3")

        chld1 = dbc.child("a")
        chld1.index("h").mark("h1")

        print("Initialization Done\n")
        return

    print("\nList Childs ...")
    res = dbc.list_childs()
    print(res)

    print("\nList Indexes ...")
    res = dbc.list_indexes()
    print(res)

    idx = dbc.index("index")
    print("\nList sub indexes ...")
    res = idx.list_indexes()
    print(res,"\n")
    for n in res:
        k = "index." + n
        l = dbc.index(k).list()
        print(k, l)

    # Marking a key
    print("\nKeys Marking ...")
    idx.mark("KEY")
    print("Keys Marked ...")
    l = idx.list()
    print(l,"\n")

    # Clearing a key
    print("Key clearing ...")
    idx.clear("KEY")
    print("Key cleared")
    l = idx.list()
    print(l)

    # Purge index
    print("\nPurging Index ...")
    idx.purge()
    print("Index purged")
    res = dbc.list_indexes()
    print(res)

    # Purge index
    chld1 = dbc.child("a")
    idv=dbc.index("h")
    print("\nPurging Index ...")
    idv.purge()
    print("Index purged")
    res = dbc.list_indexes()
    print(res)

    print()

if __name__ == "__main__":
    main()
