"""
Base class for PySpark tests.
Encapsulates the creation of the SparkSession in setUpClass
and stops the same in tearDownClass.

Developed on Apache Spark 2.x
"""
__version__ = "1.0"
__author__ = 'Prithviraj Bose'
__license__ = "BSD"
__email__ = "prithvirajbose@hotmail.com"

import sys
import os

# add pyspark and py4j libs to the PYTHONPATH
spark_home = os.environ['SPARK_HOME']
sys.path.insert(1, os.path.join(spark_home, 'python/lib/pyspark.zip'))
sys.path.insert(1, os.path.join(spark_home, 'python/lib/py4j-0.10.1-src.zip'))
