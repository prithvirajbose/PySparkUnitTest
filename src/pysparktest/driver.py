
from pyspark import SparkConf
from pyspark.sql import SparkSession
import unittest

class CustomPySparkTestCase(unittest.TestCase):
        """
        Class variables:
        spark - pyspark.sql.SparkSession
        """

        @classmethod
        def setUpClass(cls):
                cls.spark = SparkSession.builder\
                        .appName('tests')\
                        .master('local[*]')\
                        .config(conf = SparkConf())\
                        .getOrCreate()
                
        @classmethod
        def tearDownClass(cls):
                cls.spark.stop()

if __name__ == '__main__':
        unittest.main(verbosity = 2)

