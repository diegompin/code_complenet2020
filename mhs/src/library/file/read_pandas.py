"""
Author: Diego Pinheiro
github: https://github.com/diegompin

"""

import pandas as pd
import numpy as np
import multiprocessing


class ReadFilePandas(object):
    __ENCODING__ = "ISO-8859-1"
    # __ENCODING__ = 'utf-8'

    def __init__(self, filename, delimiter=',', encoding=__ENCODING__, is_multiprocessing=True):
        self.filename = filename
        self.delimiter = delimiter
        self.encoding = encoding
        self.is_multiprocessing = is_multiprocessing

    def get_data(self):
        column_mapping = self.get_column_mapping()
        # df = pd.read_table(self.filename, delimiter='\t', header=0, encoding=self.encoding, nrows=1000)
        df = pd.read_table(self.filename, delimiter=self.delimiter, header=0, encoding=self.encoding)
        df.columns = [e.strip() for e in list(df.columns)]
        if column_mapping is not None:
            df = df[list(column_mapping.values())]
            df = df.rename(columns=dict(zip(column_mapping.values(), column_mapping.keys())))
        if self.is_multiprocessing:
            df = apply_by_multiprocessing(df, self.pre_process, axis=1, workers=4)
        else:
            df = df.apply(self.pre_process, axis=1)
        return df

    def _process(self, row):
        self.pre_process(row)
        self.post_process(row)
        return row

    def pre_process(self, row):
        return row

    def post_process(self, row):
        return row

    def get_column_mapping(self):
        pass

def apply_by_multiprocessing(df, func, **kwargs):
    workers = kwargs.pop('workers')

    with multiprocessing.Pool(processes=workers) as pool:
        result = pool.map(_apply_df, [(d, func, kwargs)
                                      for d in np.array_split(df, workers)])

        return pd.concat(list(result))
    # pool = multiprocessing.Pool(processes=workers)
    #     return
    # pool.close()


def _apply_df(args):
    df, func, kwargs = args
    return df.apply(func, **kwargs)
