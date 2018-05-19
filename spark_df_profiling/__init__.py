# -*- coding: utf-8 -*-

"""Main module of spark-df-profiling"""

import codecs
import os

from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext

from spark_df_profiling.templates import template
from spark_df_profiling.describe import describe
from spark_df_profiling.report import to_html

NO_OUTPUTFILE = 'spark_df_profiling.no_outputfile'
DEFAULT_OUTPUTFILE = 'spark_df_profiling.default_outputfile'


class ProfileReport(object):
    """
    Generate a profile report from a dataset stored as a Spark `DataFrame`.

    Used has is it will output its content as an HTML report in a Jupyter notebook.

    Attributes
    ----------
    df : Spark DataFrame
        Data to be analyzed
    bins : int
        Number of bins in histogram.
        The default is 10.
    check_correlation : boolean
        Whether or not to check correlation.
        It's `True` by default.
    correlation_threshold: float
        Threshold to determine if the variable pair is correlated.
        The default is 0.9.
    correlation_overrides : list
        Variable names not to be rejected because they are correlated.
        There is no variable in the list (`None`) by default.
    check_recoded : boolean
        Whether or not to check recoded correlation (memory heavy feature).
        Since it's an expensive computation it can be activated for small datasets.
        `check_correlation` must be true to disable this check.
        It's `False` by default.

    Methods
    -------
    get_description
        Return the description (a raw statistical summary) of the dataset.
    get_rejected_variables
        Return the list of rejected variable or an empty list if there is no rejected variables.
    to_file
        Write the report to a file.
    to_html
        Return the report as an HTML string.
    """
    html = ''
    file = None

    def __init__(self, df, bins=10, sample=100, corr_reject=0.9, **kwargs):
        sample = df.limit(sample).toPandas()

        description_set = describe(df, bins=bins, corr_reject=corr_reject, **kwargs)

        self.html = to_html(sample, description_set)

        self.description_set = description_set

    def render_standalone(self, mode='databricks', utils=None):
        if mode != 'databricks':
            raise NotImplementedError('Only databricks mode is supported for now')
        else:
            library_path = os.path.abspath(os.path.dirname(__file__))
            css_path = os.path.join(library_path, 'templates/css/')
            js_path = os.path.join(library_path, 'templates/js/')
            utils.fs.mkdirs('/FileStore/spark_df_profiling/css')
            utils.fs.mkdirs('/FileStore/spark_df_profiling/js')
            utils.fs.cp('file:' + css_path + 'bootstrap-theme.min.css',
                        '/FileStore/spark_df_profiling/css/bootstrap-theme.min.css')
            utils.fs.cp('file:' + css_path + 'bootstrap.min.css',
                        '/FileStore/spark_df_profiling/css/bootstrap.min.css')
            utils.fs.cp('file:' + js_path + 'bootstrap.min.js',
                        '/FileStore/spark_df_profiling/js/bootstrap.min.js')
            utils.fs.cp('file:' + js_path + 'jquery.min.js',
                        '/FileStore/spark_df_profiling/js/jquery.min.js')
            return template('wrapper_static').render(content=self.html)

    def get_description(self):
        return self.description_set

    def get_rejected_variables(self, threshold=0.9):
        """
        Return a list of variable names being rejected for high
        correlation with one of remaining variables
        :param threshold: Correlation value which is above the threshold are rejected
        :type: float (optional)
        :return: the list of rejected variables or an empty list if the correlation has not been computed.
        :type: list
        """
        variable_profile = self.description_set['variables']
        return variable_profile.index[variable_profile.correlation > threshold].tolist()

    def to_file(self, output=DEFAULT_OUTPUTFILE):
        """
        Write the report to a file
        By default a name is generated.
        :param output: The name or the path of the file to generale including the extension (.html)
        :type: string
        """
        if output != NO_OUTPUTFILE:
            if output == DEFAULT_OUTPUTFILE:
                output = 'profile_' + str(hash(self)) + '.html'

            # TODO: should be done in the template
            with codecs.open(output, 'w+b', encoding='utf8') as self.file:
                self.file.write(template('wrapper').render(content=self.html))

    def to_html(self):
        """
        Generate and return complete template as lengthy string
        for using with frameworks
        :return: the HTML output
        :type: string
        """
        return templates.template('wrapper').render(content=self.html)

    def _repr_html_(self):
        """
        Used to output the HTML representation to a Jupyter notebook
        :return: The HTML internal representation
        :type: string
        """
        return self.html

    def __str__(self):
        """
        Overwrite of the str method.
        :return: A string representation of the object
        :type: string
        """
        return 'Output written to file ' + str(self.file.name)


def start_profiling(app_name, query, outputfile, k_vals, t_freq):
    conf = SparkConf().setAppName(app_name)
    sc = SparkContext(conf=conf)

    hive = HiveContext(sc)
    df = hive.sql(query).cache()
    ProfileReport(df, k_vals=k_vals, t_freq=t_freq).to_file(output=outputfile)
