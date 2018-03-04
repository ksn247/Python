from pyspark.sql import HiveContext, Row
from pyspark import SparkContext, SparkConf, SQLContext
import pandas as pd
import sys
import ConfigParser
reload(sys)
property_file = "config.properties"

sys.setdefaultencoding('utf-8')


class CoreDataComparision(object):
    def __init__(self):
        self.spark_conf = SparkConf.setAppName("Testing")
        self.spark_context = SparkContext(conf= self.spark_conf)
        self.hive_context = HiveContext(self.spark_context)
        self.config = ConfigParser.ConfigParser()
        self.config.read(property_file)

        self.get_hive_data()

    def get_hive_data(self):
        self.hive_context.read(self.)