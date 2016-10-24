__author__ = 'Acheron'

import pandas as pd
import time
import re

def from_file(file):
    pattern = re.compile(r'Чернівецька', re.IGNORECASE)
    for row in pd.read_csv(file, sep=';', encoding="cp1251", low_memory=False, header=0, dtype='object', chunksize=10000):
        for idx, record in row["Місцезнаходження"].iteritems():
            if re.search(pattern, str(record)):
                code = row["Код ЄДРПОУ"].loc[idx]
                status = row["Стан"].loc[idx]
                yield (idx, str(code), status, row[:].loc[idx])

def from_file_alt(file):
    pattern = re.compile(r'Чернівецька', re.IGNORECASE)
    dataframe = pd.read_csv(file, sep=';', encoding="cp1251", low_memory=False, header=0, dtype='object')
    for idx, record in dataframe["Місцезнаходження"].iteritems():
            if re.search(pattern, str(record)):
                code = dataframe["Код ЄДРПОУ"].loc[idx]
                status = dataframe["Стан"].loc[idx]
                yield (idx, str(code), status, dataframe.loc[idx])

def from_memory(dataframe):
    pattern = re.compile(r'Чернівецька', re.IGNORECASE)
    for idx, record in dataframe["Місцезнаходження"].iteritems():
            if re.search(pattern, str(record)):
                code = dataframe["Код ЄДРПОУ"].loc[idx]
                status = dataframe["Стан"].loc[idx]
                yield (idx, str(code), status, dataframe.loc[idx])

# from_file_alt('./temp/tmp_imp_stage_3.csv')