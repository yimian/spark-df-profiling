# -*- coding: utf-8 -*-

"""
Formatters are utilities for formatting numbers by adding HTML code or CSS classes
"""

import numpy as np

SKEWNESS_CUTOFF = 20
DEFAULT_FLOAT_FORMATTER = 'spark_df_profiling.__default_float_formatter'


def gradient_format(value, limit1, limit2, c1, c2):
    def lerpcolour(c1, c2, t):
        return int(c1[0] + (c2[0] - c1[0]) * t), int(c1[1] + (c2[1] - c1[1]) * t), int(c1[2] + (c2[2] - c1[2]) * t)

    c = lerpcolour(c1, c2, (value - limit1) / (limit2 - limit1))
    return fmt_color(value, "rgb{}".format(str(c)))


def fmt_color(text, color):
    return u'<span style="color:{color}">{text}</span>'.format(color=color, text=str(text))


def fmt_class(text, cls):
    return u'<span class="{cls}">{text}</span>'.format(cls=cls, text=str(text))


def fmt_bytesize(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)


def fmt_percent(v):
    return "{:2.1f}%".format(v * 100)


def fmt_varname(v):
    return u'<code>{0}</code>'.format(v)


value_formatters = {
    'freq': (lambda v: gradient_format(v, 0, 62000, (30, 198, 244), (99, 200, 72))),
    'p_missing': fmt_percent,
    'p_infinite': fmt_percent,
    'p_unique': fmt_percent,
    'p_zeros': fmt_percent,
    'memorysize': fmt_bytesize,
    'total_missing': fmt_percent,
    DEFAULT_FLOAT_FORMATTER: lambda v: str(float('{:.5g}'.format(v))).rstrip('0').rstrip('.'),
    'correlation_var': lambda v: fmt_varname(v),
    'accuracy_idx': fmt_percent
}


def fmt_row_severity(v):
    if np.isnan(v) or v <= 0.01:
        return "ignore"
    return "alert"


def fmt_skewness(v):
    if not np.isnan(v) and (v < -SKEWNESS_CUTOFF or v > SKEWNESS_CUTOFF):
        return "alert"
    return ""


row_formatters = {
    'p_zeros': fmt_row_severity,
    'p_missing': fmt_row_severity,
    'p_infinite': fmt_row_severity,
    'n_duplicates': fmt_row_severity,
    'skewness': fmt_skewness,
}
