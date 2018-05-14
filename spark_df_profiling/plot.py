# -*- coding: utf-8 -*-

import base64
try:
    from StringIO import BytesIO
except ImportError:
    from io import BytesIO

try:
    from urllib import quote
except ImportError:
    from urllib.parse import quote

from matplotlib import pyplot as plt


BASE = 'data:image/png;base64,'


def mini_histogram(hist_data):
    """Small histogram"""
    img_data = BytesIO()
    plt.figure(figsize=(2, 0.75))
    plot = plt.subplot()
    plt.bar(hist_data['left_edge'], hist_data['count'], width=hist_data['width'], facecolor='#337ab7')
    plot.axes.get_yaxis().set_visible(False)
    plot.set_axis_bgcolor('w')
    xticks = plot.xaxis.get_major_ticks()
    for tick in xticks[1:-1]:
        tick.set_visible(False)
        tick.label.set_visible(False)
    for tick in (xticks[0], xticks[-1]):
        tick.label.set_fontsize(8)
    plot.figure.subplots_adjust(left=0.15, right=0.85, top=1, bottom=0.35, wspace=0, hspace=0)
    plot.figure.savefig(img_data)
    img_data.seek(0)
    result_string = BASE + quote(base64.b64encode(img_data.getvalue()))
    plt.close(plot.figure)
    return result_string


def complete_histogram(hist_data):
    """Large histogram"""
    img_data = BytesIO()
    plt.figure(figsize=(6, 4))
    plot = plt.subplot()
    plt.bar(hist_data['left_edge'], hist_data['count'], width=hist_data['width'], facecolor='#337ab7')
    plot.set_ylabel('Frequency')
    plot.figure.subplots_adjust(left=0.15, right=0.95, top=0.9, bottom=0.1, wspace=0, hspace=0)
    plot.figure.savefig(img_data)
    img_data.seek(0)
    result_string = BASE + quote(base64.b64encode(img_data.getvalue()))
    # TODO Think about writing this to disk instead of caching them in strings
    plt.close(plot.figure)
    return result_string

