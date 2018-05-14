# -*- coding: utf-8 -*-

"""Generate reports"""

import six
import sys

import pandas as pd

import spark_df_profiling.formatters as formatters
import spark_df_profiling.templates as templates


def value_format(value, name):
    value_formatters = formatters.value_formatters
    if pd.isnull(value):
        return ''
    if name in value_formatters:
        return value_formatters[name](value)
    elif isinstance(value, float):
        return value_formatters[formatters.DEFAULT_FLOAT_FORMATTER](value)
    else:
        if sys.version_info.major == 3:
            return str(value)
        else:
            return unicode(value)


def format_freq_table(varname, freqtable, n, var_table, table_template, row_template, max_number_of_items_in_table):
    freq_other_prefiltered = freqtable['***Other Values***']
    freq_other_prefiltered_num = freqtable['***Other Values Distinct Count***']
    freqtable = freqtable.drop(['***Other Values***', '***Other Values Distinct Count***'])
    freq_other = sum(freqtable[max_number_of_items_in_table:]) + freq_other_prefiltered
    freq_missing = var_table['n_missing']
    max_freq = max(freqtable.values[0], freq_other, freq_missing)
    try:
        min_freq = freqtable.values[max_number_of_items_in_table]
    except IndexError:
        min_freq = 0

    # TODO: Correctly sort missing and other
    def format_row(f, l, extra_class=''):
        width = int(f / float(max_freq) * 99) + 1
        if width > 20:
            label_in_bar = f
            label_after_bar = ""
        else:
            label_in_bar = '&nbsp;'
            label_after_bar = f

        return row_template.render(label=l,
                                   width=width,
                                   count=f,
                                   percentage='{:2.1f}'.format(f / float(n) * 100),
                                   extra_class=extra_class,
                                   label_in_bar=label_in_bar,
                                   label_after_bar=label_after_bar)

    freq_rows_html = ''
    for label, freq in six.iteritems(freqtable[0:max_number_of_items_in_table]):
        freq_rows_html += format_row(freq, label)

    if freq_other > min_freq:
        freq_rows_html += format_row(freq_other,
                                     'Other values (%s)' % (freqtable.count()
                                                            + freq_other_prefiltered_num
                                                            - max_number_of_items_in_table),
                                     extra_class='other')

    if freq_missing > min_freq:
        freq_rows_html += format_row(freq_missing, '(Missing)', extra_class='missing')

    return table_template.render(rows=freq_rows_html, varid=hash(varname))


def to_html(sample, stats_object):
    """
    Generate a HTML report from summary statistics and a given sample
    :param sample: DataFrame containing the sample you want to print
    :param stats_object: Dictionary containing summary statistics. Should be generated with an appropriate describe() function
    :return: profile report in HTML format
    :type: string
    """
    n_obs = stats_object['table']['n']
    row_formatters = formatters.row_formatters

    if not isinstance(sample, pd.DataFrame):
        raise TypeError('sample must be of type pandas.DataFrame')

    if not isinstance(stats_object, dict):
        raise TypeError('stats_object must be of type dict. Did you generate this using the spark_df_profiling.describe() function?')

    if set(stats_object.keys()) != {'table', 'variables', 'freq'}:
        raise TypeError('stats_object badly formatted. Did you generate this using the spark_df_profiling-eda.describe() function?')

    # Variables
    rows_html = ''
    messages = []

    for idx, row in stats_object['variables'].iterrows():

        formatted_values = {'varname': idx, 'varid': hash(idx)}
        row_classes = {}

        for col, value in six.iteritems(row):
            formatted_values[col] = value_format(value, col)

        for col in set(row.index) & six.viewkeys(row_formatters):
            row_classes[col] = row_formatters[col](row[col])
            if row_classes[col] == 'alert' and col in templates.messages:
                messages.append(templates.messages[col].format(formatted_values, varname=formatters.fmt_varname(idx)))

        if row['type'] == 'CAT':
            formatted_values['minifreqtable'] = format_freq_table(idx, stats_object['freq'][idx], n_obs,
                                                                  stats_object['variables'].ix[idx],
                                                                  templates.template('mini_freq_table'),
                                                                  templates.template('mini_freq_table_row'), 3)
            formatted_values['freqtable'] = format_freq_table(idx, stats_object['freq'][idx], n_obs,
                                                              stats_object['variables'].ix[idx],
                                                              templates.template('freq_table'),
                                                              templates.template('freq_table_row'), 20)
            if row['distinct_count'] > 50:
                messages.append(templates.messages['HIGH_CARDINALITY'].format(formatted_values,
                                                                              varname=formatters.fmt_varname(idx)))
                row_classes['distinct_count'] = 'alert'
            else:
                row_classes['distinct_count'] = ''

        if row['type'] == 'UNIQUE':
            obs = stats_object['freq'][idx].index

            formatted_values['firstn'] = pd.DataFrame(obs[0:3], columns=['First 3 values']).to_html(
                classes='example_values', index=False)
            formatted_values['lastn'] = pd.DataFrame(obs[-3:], columns=['Last 3 values']).to_html(
                classes='example_values', index=False)

            if n_obs > 40:
                formatted_values['firstn_expanded'] = pd.DataFrame(obs[0:20], index=range(1, 21)).to_html(
                    classes='sample table table-hover', header=False)
                formatted_values['lastn_expanded'] = pd.DataFrame(obs[-20:],
                                                                  index=range(n_obs - 20 + 1, n_obs + 1)).to_html(
                    classes='sample table table-hover', header=False)
            else:
                formatted_values['firstn_expanded'] = pd.DataFrame(obs, index=range(1, n_obs + 1)).to_html(
                    classes='sample table table-hover', header=False)
                formatted_values['lastn_expanded'] = ''

        rows_html += templates.row_templates_dict[row['type']].render(values=formatted_values, row_classes=row_classes)

        if row['type'] in ['CORR', 'CONST']:
            formatted_values['varname'] = formatters.fmt_varname(idx)
            messages.append(templates.messages[row['type']].format(formatted_values))

    # Overview
    formatted_values = {k: value_format(v, k) for k, v in six.iteritems(stats_object['table'])}

    row_classes = {}
    for col in six.viewkeys(stats_object['table']) & six.viewkeys(row_formatters):
        row_classes[col] = row_formatters[col](stats_object['table'][col])
        if row_classes[col] == 'alert' and col in templates.messages:
            messages.append(templates.messages[col].format(formatted_values, varname=formatters.fmt_varname(idx)))

    messages_html = ''
    for msg in messages:
        messages_html += templates.message_row.format(message=msg)

    overview_html = templates.template('overview').render(values=formatted_values, row_classes=row_classes, messages=messages_html)

    # Add Sample
    sample_html = templates.template('sample').render(sample_table_html=sample.to_html(classes='sample'))
    # TODO: should be done in the template
    return templates.template('base').render({'overview_html': overview_html, 'rows_html': rows_html, 'sample_html': sample_html})
