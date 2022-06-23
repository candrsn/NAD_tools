

from statistics import mode
import sys
import os
import sqlite3
import sqlalchemy
import pandas
import zipfile
import time

import logging

logger = logging.getLogger(__name__)


# use a decorator to inject some SQLITE3 PRAGMA's into the DBAPI connection
@sqlalchemy.event.listens_for(sqlalchemy.engine.Engine, "connect")
def sqlite_conn_setup(conn, conn_record):
    cur=conn.cursor()
    cur.execute("PRAGMA synchronous=OFF")
    cur.execute("PRAGMA journal_mode=OFF")
    cur.close()


def import_txt(rel="r10"):
    tbl = "nad_addresses"
    zff = zipfile.ZipFile(f"{rel}/NAD_{rel}_TXT.zip")

    sqleng = sqlalchemy.create_engine(f"sqlite:////usbmedia/gis_data/tmp/nad_{rel}.dbt")
    itr = 0

    # read a CSV in the ZIP File in chunks of 1 million entries
    for df in pandas.read_csv(zff.open("TXT/NAD_r10.txt"), dtype=str, chunksize=1000000):
        logger.info(df.shape)
        ts = time.time()
        #result = df.to_sql(tbl, sqleng, if_exists="append", method="multi", chunksize=4000)
        # method+multi is very slow on SQLITE3
        result = df.to_sql(tbl, sqleng, if_exists="append", index=False, chunksize=4000)
        logger.info(f"itr {itr}: {result} records inserted {df.shape} in {(time.time() - ts):-0.5g} seconds,  {(result/(time.time() - ts)):-0.15g} rec/sec")
        itr += 1


def main(args):
    import_txt()

    logger.info("All Done")


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)

    main(sys.argv)

