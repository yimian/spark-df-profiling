# -*- coding: utf-8 -*-

from itertools import product
import pandas as pd


def pretty_name(x):
    x *= 100
    if x == int(x):
        return '%.0f%%' % x
    else:
        return '%.1f%%' % x


def separate(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def corr_matrix(df, columns=None):
    """Function to compute the correlation matrix"""
    if columns is None:
        columns = df.columns
    combinations = list(product(columns, columns))

    grouped = list(separate(combinations, len(columns)))
    df_cleaned = df.select(*columns).na.drop(how="any")

    for i in grouped:
        for j in enumerate(i):
            i[j[0]] = i[j[0]] + (df_cleaned.corr(str(j[1][0]), str(j[1][1])),)

    df_pandas = pd.DataFrame(grouped).applymap(lambda x: x[2])
    df_pandas.columns = columns
    df_pandas.index = columns
    return df_pandas
