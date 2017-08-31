
from pysparktest.driver import CustomPySparkTestCase
import unittest

class SampleTest(CustomPySparkTestCase):
    def test_word_cnt(self):
        rdd = self.spark.sparkContext.parallelize(['Hi there', 'Hi'])
        self.assertEqual(word_cnt(rdd).collectAsMap(), {'Hi' : 2, 'there' : 1})
     
def word_cnt(rdd):
    return (rdd.flatMap(lambda line: line.split(' '))
     .map(lambda word: (word, 1))
     .reduceByKey(lambda acc, cnt: acc + cnt))

if __name__ == '__main__':
        unittest.main(verbosity = 2)
