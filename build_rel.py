

from statistics import mode
import sys
import os
import sqlite3
import sqlalchemy
import pandas
import zipfile

import logging

logger = logging.getLogger(__name__)


def import_txt(rel="r10"):
    tbl = "nad_addresses"
    zff = zipfile.ZipFile(f"{rel}/NAD_{rel}_TXT.zip")

    sqleng = sqlalchemy.create_engine(f"sqlite:///nad_{rel}.db")
    for df in pandas.read_csv(zff.open("TXT/NAD_r10.txt") , chunksize=10000):
        # logger.info(df.head(5))
        result = df.to_sql(tbl, sqleng, if_exists="append", method="multi")
        logger.info(f"{result} records inserted {df.shape}")

def main(args):
    import_txt()

    logger.info("All Done")


if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)

    main(sys.argv)

