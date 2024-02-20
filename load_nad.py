
import sys
import os
import sqlite3
import pyarrow
import pyarrow.parquet
import pandas
import numpy
import glob
import time
import zipfile
import datetime
import logging
import subprocess
import warnings
import nad_storage
from collections import defaultdict

logger = logging.getLogger(__name__)

# need to ignore all warnings to filter those coming from pandas
warnings.filterwarnings("ignore")


# doing this slows down the import in a great way
# but doing it here is more consistent and overall quicker
# once the time to postfix the format is added
def nad_timestamp_parser(data):
    # special null values get handled here
    if data in ('NaT', 'None', 'NONE', None, 'Nan', ''):
        return None

    data2 = datetime.datetime.strptime(data, '%m/%d/%Y %H:%M:%S').isoformat()

    return data2


def load(db, TXTfile, rel="R10", chk_size=50000):

    # declare an iterator to read through the input file
    #col_types = defaultdict(str, LastUpdate="Timestamp", Effective="Timestamp", Expired="Timestamp")
    col_types =  str
    df = pandas.read_csv(TXTfile, sep=',', iterator=True, chunksize=chk_size,
            engine='c',
            dtype=col_types,
            keep_default_na=False,
            na_values=["None", "NaN", "NaT", "NULL", "Null"]
            , converters = {"LastUpdate": nad_timestamp_parser, 
                 "Effective": nad_timestamp_parser, 
                 "Expired":nad_timestamp_parser}
    )

    itr_cnt = 0
    tbl = f"NAD_{rel}"
    stime = time.time()

    for chk in df:
        if itr_cnt == 0:
            logger.info(chk.head())
            logger.info(chk.shape)

        # only create the table on the first pass, and append afterwards
        # sqlite3 is picky about commit windows
        db.write(tbl, chk)
        logger.info(f"process chunk {itr_cnt} in {(time.time()-stime):5.5g} sec")

        itr_cnt += 1
        if itr_cnt > 0 and itr_cnt % 20 == 0:
            db.commit()
            s = str(chk.head(1)["State"]).split("\n")[0].split(" ")[-1]
            logger.info(f""" commit at {itr_cnt * chk_size} records read, now at state: {s}""")
        stime = time.time()

    return itr_cnt


def check_archive_contents(nadzip, nadtarget):
    zff = zipfile.ZipFile(nadzip)

    # test if the provided name exists
    if not nadtarget in zff.namelist():
        ntsize = 0
        for itm in zff.infolist():
            # otherwise grab the largest txt file in the archive
            if itm.filename.endswith(".txt") and itm.file_size > ntsize:
                nadtarget = itm.filename
                ntsize = itm.file_size
 
    zff.close()
    return nadtarget


def check_archive(nadzip):
    if os.path.exists(nadzip):
        return nadzip
    
    for itm in glob.glob(os.path.join(os.path.dirname(nadzip),"*zip")):
        if 'TXT' in itm or 'ASCII' in itm:
            return itm
        
    assert False, f"Failed to find a candidate archive for {nadzip}"


def setup_fifo(nadzip, nadtarget, fifoname):
    ## pandas already knows how to read from ZIP files, 
    # need to get that syntax correctly understood
    # and see if it does threading
    # or investigate dask
    newid = os.fork()

    # newid == 0 is the child process continuing after the parent exits this loop
    # os.fork() returns 0 when running in the child's process
    if newid == 0:
        nt2 = check_archive_contents(nadzip, nadtarget)
        if not os.path.exists(fifoname):
            os.system(f"mkfifo {fifoname}")
        os.system(f"unzip -p {nadzip} {nt2}  > {fifoname}")

        # now stop this thread
        sys.exit()


def report_db_simple(db, rel):
    cur = db.cursor()

    cur.execute(f"SELECT max(rowid) FROM NAD_{rel}")
    data = cur.fetchall()
    datac = data[0][0] / (1000 * 1000.0)

    logger.info(f"Rel {rel} has {(datac):-0.7g} million addresses")


def report_db_detail(db, rel):
    cur = db.cursor()

    cur.execute(f"SELECT count(*), state, min(lastupdate), max(lastupdate) FROM NAD_{rel} GROUP BY 2 ORDER BY 2")
    data = cur.fetchall()

    for row in data:
        datac = row[0] / (1000 * 1000.0)
        logger.info(f"Rel {rel} in State: {row[1]} has {(datac):-0.7g} million addresses, times: {row[2]} - {row[3]}")


def report_db(dbfile, rel, detailed=False):
    db = sqlite3.connect(dbfile)

    report_db_simple(db, rel)
    if detailed is True:
        report_db_detail(db, rel)


def load_db(dbc, rel, fifoname, chksz):
    logger.info(f"Starting load of NAD data with rel:{rel}, db:{dbc.db_name}, fifo:{fifoname}")

    stime = time.time()

    archive_file = f"{rel}/NAD_{rel}_TXT.zip"
    archive_file = check_archive(archive_file)
    setup_fifo(archive_file, "TXT/NAD.txt", fifoname)
    # The parent PID will wait for the FIFO to have content
    # So no need to wait for it to become ready
    # but be a little cautious
    time.sleep(2)
 

    # ~/tmp/nadd is a unix FIFO containing the stream of unzip -p NAD_r13_TXT.zip TXT/NAD.txt
    # this speed up reading a great deal
    loops = load(dbc, fifoname, rel=rel.upper(), chk_size=chksz)
    logger.info(f"Loaded in {((time.time() - stime)/60.0):-0.5g} minutes, {loops} loops of {chksz}")


def main(args):
    if "--release" in args:
        rel = args[args.index("--release")+1]
    else:
        rel="r15"

    if "--fifoname" in args:
        fifoname = os.path.expanduser(args[args.index("--fifoname")+1])
        customfifo = True
    else:
        fifoname = os.path.expanduser(f"tmp/nadd")
        customfifo = False

    if "--db" in args:
        dbfile = os.path.expanduser(args[args.index("--db")+1])
        if dbfile.split('.')[-1] not in ('db','sqlite','sqlite3'):
            dbfile += ".db"
    else:
        dbfile = os.path.expanduser(f"tmp/nad_{rel}_q.db")

    if "--report" in args:
        dx = "--detailed" in args
        report_db(dbfile, rel, detailed=dx)
        sys.exit()

    chksz = 150000

    #dbc = nad_storage.Sqlite_NAD_Writer(dbfile, chunksize=chksz)
    dbc = nad_storage.Parquet_NAD_Writer(f"tmp/nad_{rel}_q/nad_{rel}", chunksize=chksz, compression="ZSTD")

    load_db(dbc, rel, fifoname, chksz)

    if customfifo is True:
        os.remove(fifoname)

    logger.info("All Done")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main(sys.argv)