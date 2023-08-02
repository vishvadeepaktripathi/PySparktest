from operator import add

from PySparkTest import PySparkTest


class SimpleTest(PySparkTest):
    def test_basic(self):
        spark = self.spark.sparkContext.parallelize(['cat dog mouse', 'cat cat dog'], 2)
        results = spark.flatMap(lambda line: line.split()) \
        .map(lambda word: (word,1)) \
        .reduceByKey(add).collect()
        expected_result = [('cat', 3),('dog', 2), ('mouse', 1)]
        self.assertEquals(set(results))\
            , set(expected_result)
