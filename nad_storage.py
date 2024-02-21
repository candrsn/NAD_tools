


import sys
import os

import pyarrow
import pyarrow.parquet
import sqlite3
import glob
import logging

logger = logging.getLogger(__name__)



class Sqlite_NAD_Writer():
    db = None
    cur = None
    file_path = None
    db_chunk = 50000

    def __init__(self, dbfile, chunksize=50000):
        self.db = sqlite3.connect(dbfile)
        self.db_chunk = chunksize
        self.db = sqlite3.connect(dbfile)
        self.db_name = dbfile
        self.setup_conn(self.db)

    def connect(self):
        return self.db

    def write(self, tbl, df):
        df.to_sql(tbl, self.db, if_exists="append", index=None, chunksize=self.db_chunk)

    def compute_set_metadata(self):
        pass

    def commit(self):
        self.db.commit()

    def close(self):
        self.db.close()

    def __setup_conn__(conn):
        cur=conn.cursor()
        cur.execute("PRAGMA synchronous=OFF")
        cur.execute("PRAGMA journal_mode=OFF")
        cur.close()



class Parquet_NAD_Writer():
    pattern = None
    active_file = None
    file_iter = 0
    files = []
    writer = None
    schema = None
    file_rowgroups = 100
    rowset_itr = 0

    def __init__(self, pathpattern, chunksize=None, compression="ZSTD", 
                compression_level=9, row_group_size=100):
        self.pattern = pathpattern
        self.active_file = self.compute_next_filename()
        self.chunksize = chunksize
        self.compression = compression
        self.compression_level = compression_level
        self.file_rowgroups = row_group_size
        fparts = pathpattern.split('/')
        if len(fparts) > 1:
            os.makedirs('/'.join(fparts[:-1]), exist_ok=True)

    def compute_next_filename(self):
        if self.writer is not None:
            self.writer.close()

        fx = f"{self.pattern}_part_{self.file_iter:-06d}.parquet"
        self.active_file = fx
        self.db_name = fx
        self.writer = None
        self.file_iter += 1
        
        return fx

    def close_writer(self):
        if self.writer is not None:
            self.writer.close()
        os.sync()

    def connect(self):
        pass

    def write(self, tbl, df):
        if self.schema is None:
            self.schema = pyarrow.schema([
                pyarrow.field(name, pyarrow.string()) for name in df.columns])

        fx = pyarrow.Table.from_pandas(df, schema=self.schema, safe=False)
        if self.writer is None:
            self.writer = pyarrow.parquet.ParquetWriter(self.active_file, fx.schema, 
                            compression=self.compression, compression_level=self.compression_level)
        self.writer.write_table(fx)

        if self.rowset_itr > self.file_rowgroups:
            self.rowset_itr = 0
            self.compute_next_filename()
        else:
            self.rowset_itr += 1

    def compute_set_metadata(self):
        if "_metadata" in glob.glob(self.pattern + "/*"):
            fileset = pyarrow.parquet.ParquetDataset(self.pattern)
        else:
            fg = glob.glob(self.pattern + "/*.parquet")
            fileset = pyarrow.parquet.ParquetDataset(fg)

            schema = fileset.schema

            mc = []
            pyarrow.parquet.write_metadata(schema, self.pattern + "/_metadata", metadata_collector=mc)

    def commit(self):
        os.sync()

    def close(self):
        if self.writer is not None:
            self.writer.close()
        self.compute_set_metadata()




def parquet_stats(path):
    os.sync()

    if not os.path.isdir(path):
        path = os.path.dirname(path)

    fg = glob.glob(path + "/*.parquet")

    if os.path.exists(path + "/_metadata"):
        fileset = pyarrow.parquet.ParquetDataset(path)
    else:
        fileset = pyarrow.parquet.ParquetDataset(fg)

        schema = fileset.schema

        mc = []
        pyarrow.parquet.write_metadata(schema, path + "/_metadata", metadata_collector=mc)

    for pqfile in fileset.files:
        meta = pyarrow.parquet.read_metadata(pqfile)
        print(f"pqfile \n")
        print(meta)
        csz = 0
        ucsz = 0
        for rg in range(0, meta.num_row_groups):
            for cg in range(meta.num_columns):
                colmeta = meta.row_group(rg).column(cg)
                ucsz += colmeta.total_uncompressed_size
                csz += colmeta.total_compressed_size

        print(f"file compression ratio {float(ucsz / csz):-5.5g} \n\n")
        #print(meta.row_group(0).column(2))
        #print(meta.row_group(3).column(9))



if __name__ == "__main__":

    parquet_stats("tmp/nad_r5")

