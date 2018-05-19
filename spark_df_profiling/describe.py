# -*- coding: utf-8 -*-

"""Compute statistical description of datasets"""

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
from pkg_resources import resource_filename

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import abs, col, count, countDistinct,\
    max, mean, min, sum, when, variance, stddev, kurtosis, skewness

import spark_df_profiling.formatters as formatters
from spark_df_profiling.utils import pretty_name, corr_matrix
from spark_df_profiling.plot import mini_histogram, complete_histogram


def create_all_conditions(current_col, column, left_edges, count=1):
    """
    Recursive function that exploits the
    ability to call the Spark SQL Column method
    .when() in a recursive way
    """
    left_edges = left_edges[:]
    if len(left_edges) == 0:
        return current_col
    if len(left_edges) == 1:
        next_col = current_col.when(col(column) >= float(left_edges[0]), count)
        left_edges.pop(0)
        return create_all_conditions(next_col, column, left_edges[:], count + 1)
    next_col = current_col.when((float(left_edges[0]) <= col(column)) & (col(column) < float(left_edges[1])), count)
    left_edges.pop(0)
    return create_all_conditions(next_col, column, left_edges[:], count + 1)


def generate_hist_data(df, column, minim, maxim, bins=10):
    """Compute histogram (is not as easy as it looks)"""
    num_range = maxim - minim
    bin_width = num_range / float(bins)
    left_edges = [minim]
    for _bin in range(bins):
        left_edges = left_edges + [left_edges[-1] + bin_width]
    left_edges.pop()
    expression_col = when((float(left_edges[0]) <= col(column)) & (col(column) < float(left_edges[1])), 0)
    left_edges_copy = left_edges[:]
    left_edges_copy.pop(0)
    bin_data = (df.select(col(column)).na.drop().select(col(column), create_all_conditions(expression_col, column, left_edges_copy).alias('bin_id')).groupBy('bin_id').count()).toPandas()

    # If no data goes into one bin, it won't
    # appear in bin_data; so we should fill in the blanks:
    bin_data.index = bin_data['bin_id']
    new_index = list(range(bins))
    bin_data = bin_data.reindex(new_index)
    bin_data['bin_id'] = bin_data.index
    bin_data = bin_data.fillna(0)

    # We add the left edges and bin width:
    bin_data['left_edge'] = left_edges
    bin_data['width'] = bin_width

    return bin_data


def describe(df, bins=10, corr_reject=0.9, **kwargs):
    if not isinstance(df, SparkDataFrame):
        raise TypeError('df must be of type pyspark.sql.DataFrame')

    # Number of rows:
    table_stats = {'n': df.count()}
    if table_stats['n'] == 0:
        raise ValueError('df cannot be empty')

    try:
        # reset matplotlib style before use
        # Fails in matplotlib 1.4.x so plot might look bad
        matplotlib.style.use('default')
    except:
        pass

    matplotlib.style.use(resource_filename(__name__, 'spark_df_profiling.mplstyle'))

    # Data profiling
    k_vals, t_freq = kwargs.get('k_vals') or {}, kwargs.get('t_freq') or {}
    ldesc = {column: describe_1d(df, bins, column, table_stats['n'], k_vals, t_freq) for column in df.columns}

    # Compute correlation matrix
    if corr_reject is not None:
        computable_corrs = [column for column in ldesc if ldesc[column]['type'] == 'NUM']

        if len(computable_corrs) > 0:
            corr = corr_matrix(df, columns=computable_corrs)
            for x, corr_x in corr.iterrows():
                for y, corr in corr_x.iteritems():
                    if x == y:
                        break

                    if corr >= corr_reject:
                        ldesc[x] = pd.Series(['CORR', y, corr], index=['type', 'correlation_var', 'correlation'], name=x)

    # Convert ldesc to a DataFrame
    variable_stats = pd.DataFrame(ldesc)

    # General statistics
    table_stats['nvar'] = len(df.columns)
    table_stats['total_missing'] = float(variable_stats.ix['n_missing'].sum()) / (table_stats['n'] * table_stats['nvar'])
    table_stats['accuracy_idx'] = 1 - ((variable_stats.ix['high_idx'] + variable_stats.ix['low_idx']) / variable_stats.ix['count']).mean(skipna=True)
    memsize = 0
    table_stats['memsize'] = formatters.fmt_bytesize(memsize)
    table_stats['recordsize'] = formatters.fmt_bytesize(memsize / table_stats['n'])
    table_stats.update({k: 0 for k in ('NUM', 'DATE', 'CONST', 'CAT', 'UNIQUE', 'CORR')})
    table_stats.update(dict(variable_stats.loc['type'].value_counts()))
    table_stats['REJECTED'] = table_stats['CONST'] + table_stats['CORR']

    freq_dict = {}
    for var in variable_stats:
        if 'value_counts' not in variable_stats[var]:
            pass
        elif variable_stats[var]['value_counts'] is not np.nan:
            freq_dict[var] = variable_stats[var]['value_counts']
        else:
            pass
    try:
        variable_stats = variable_stats.drop('value_counts')
    except ValueError:
        pass

    return {
        'table': table_stats,
        'variables': variable_stats.T,
        'freq': freq_dict
    }


def describe_1d(df, bins, column, nrows, k_vals, t_freq):
    column_type = df.select(column).dtypes[0][1]
    # TODO: think about implementing analysis for complex
    # Special data types:
    if ('array' in column_type) or ('struct' in column_type) or ('map' in column_type):
        raise NotImplementedError('Column {c} is of type {t} and cannot be analyzed'.format(c=column, t=column_type))

    distinct_count = df.select(column).agg(countDistinct(col(column)).alias('distinct_count')).toPandas()
    non_nan_count = df.select(column).na.drop().select(count(col(column)).alias('count')).toPandas()
    results_data = pd.concat([distinct_count, non_nan_count], axis=1)
    results_data['p_unique'] = results_data['distinct_count'] / float(results_data['count'])
    results_data['is_unique'] = results_data['distinct_count'] == nrows
    results_data['n_missing'] = nrows - results_data['count']
    results_data['p_missing'] = results_data['n_missing'] / float(nrows)
    results_data['p_infinite'] = 0
    results_data['n_infinite'] = 0
    result = results_data.ix[0].copy()
    result['memorysize'] = 0
    result.name = column

    k, freq = k_vals.get(column, 2), t_freq.get(column, 'D')
    if result['distinct_count'] <= 1:
        result = result.append(describe_constant_1d(df, column))
    elif column_type in ['tinyint', 'smallint', 'int', 'bigint']:
        result = result.append(describe_numeric_1d(df, bins, column, result, nrows, k, dtype='int'))
    elif column_type in ['float', 'double', 'decimal']:
        result = result.append(describe_numeric_1d(df, bins, column, result, nrows, k, dtype='float'))
    elif column_type in ['date', 'timestamp']:
        result = result.append(describe_date_1d(df, column, result['distinct_count'], freq=freq.upper()))
    elif result['is_unique']:
        result = result.append(describe_unique_1d(df, column))
    else:
        result = result.append(describe_categorical_1d(df, column))
        # Fix to also count MISSING value in the distinct_count field:
        if result['n_missing'] > 0:
            result['distinct_count'] += 1

    # TODO: check whether it is worth it to
    # implement the 'real' mode:
    if result['count'] > result['distinct_count'] > 1:
        try:
            result['mode'] = result['top']
        except KeyError:
            result['mode'] = 0
    else:
        try:
            result['mode'] = result['value_counts'].index[0]
        except KeyError:
            result['mode'] = 0
        # If and IndexError happens,
        # it is because all column are NULLs:
        except IndexError:
            result['mode'] = 'MISSING'

    return result


def describe_numeric_1d(df, bins, column, current_result, nrows, k=2, dtype='int'):
    stats_df = df.select(column).na.drop().agg(mean(col(column)).alias('mean'),
                                               min(col(column)).alias('min'),
                                               max(col(column)).alias('max'),
                                               variance(col(column)).alias('variance'),
                                               kurtosis(col(column)).alias('kurtosis'),
                                               stddev(col(column)).alias('std'),
                                               skewness(col(column)).alias('skewness'),
                                               sum(col(column)).alias('sum')
                                               ).toPandas()

    if dtype.lower() == 'int':
        select_expr = 'percentile({c},CAST({p} AS DOUBLE))'
    else:
        select_expr = 'percentile_approx({c},CAST({p} AS DOUBLE))'
    for p in [0.05, 0.25, 0.5, 0.75, 0.95]:
        stats_df[pretty_name(p)] = (df.select(column).na.drop().selectExpr(select_expr.format(c=column, p=p)).toPandas().ix[:, 0])
    stats = stats_df.ix[0].copy()
    stats.name = column
    stats['range'] = stats['max'] - stats['min']
    q3, q1 = stats[pretty_name(0.75)], stats[pretty_name(0.25)]
    stats['iqr'] = q3 - q1
    stats['cv'] = stats['std'] / float(stats['mean'])
    stats['mad'] = (df.select(column)
                    .na.drop()
                    .select(abs(col(column) - stats['mean']).alias('delta'))
                    .agg(sum(col('delta'))).toPandas().ix[0, 0] / float(current_result['count']))
    stats['type'] = 'NUM'
    stats['n_zeros'] = df.select(column).where(col(column) == 0.0).count()
    stats['p_zeros'] = stats['n_zeros'] / float(nrows)
    stats['high_idx'] = df.select(column).where(col(column) > q3 + k * (q3 - q1)).count()
    stats['low_idx'] = df.select(column).where(col(column) < q1 - k * (q3 - q1)).count()

    # generate histograms
    hist_data = generate_hist_data(df, column, stats['min'], stats['max'], bins)
    stats['histogram'] = complete_histogram(hist_data)
    stats['mini_histogram'] = mini_histogram(hist_data)
    return stats


def describe_date_1d(df, column, distinct_count, freq='D'):
    stats_df = df.select(column).na.drop().agg(min(col(column)).alias('min'), max(col(column)).alias('max')).toPandas()
    stats = stats_df.ix[0].copy()
    stats.name = column

    # Convert Pandas timestamp object to regular datetime:
    if isinstance(stats['max'], pd.Timestamp):
        stats = stats.astype(object)
        stats['max'] = str(stats['max'].to_pydatetime())
        stats['min'] = str(stats['min'].to_pydatetime())
    # Range only got when type is date
    else:
        stats['range'] = stats['max'] - stats['min']
    stats['type'] = 'DATE'
    stats['completeness_idx'] = float(distinct_count) / len(pd.date_range(start=stats['min'], end=stats['max'], freq=freq))
    return stats


def describe_categorical_1d(df, column):
    value_counts = (df.select(column).na.drop().groupBy(column).agg(count(col(column))).orderBy('count({c})'.format(c=column), ascending=False)).cache()

    # Get the most frequent class:
    stats = (value_counts.limit(1).withColumnRenamed(column, 'top').withColumnRenamed('count({c})'.format(c=column), 'freq')).toPandas().ix[0]

    # Get the top 50 classes by value count,
    # and put the rest of them grouped at the
    # end of the Series:
    top_50 = value_counts.limit(50).toPandas().sort_values('count({c})'.format(c=column), ascending=False)
    top_50_categories = top_50[column].values.tolist()

    others_count = pd.Series([df.select(column).na.drop().where(~(col(column).isin(*top_50_categories))).count()], index=['***Other Values***'])
    others_distinct_count = pd.Series([value_counts.where(~(col(column).isin(*top_50_categories))).count()], index=['***Other Values Distinct Count***'])

    top = top_50.set_index(column)['count({c})'.format(c=column)]
    top = top.append(others_count)
    top = top.append(others_distinct_count)
    stats['value_counts'] = top
    stats['type'] = 'CAT'
    value_counts.unpersist()
    return stats


def describe_constant_1d(df, column):
    stats = pd.Series(['CONST'], index=['type'], name=column)
    stats['value_counts'] = (df.select(column).na.drop().limit(1)).toPandas().ix[:, 0].value_counts()
    return stats


def describe_unique_1d(df, column):
    stats = pd.Series(['UNIQUE'], index=['type'], name=column)
    stats['value_counts'] = (df.select(column).na.drop().limit(50)).toPandas().ix[:, 0].value_counts()
    return stats
