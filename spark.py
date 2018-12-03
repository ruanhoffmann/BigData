from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("WordCount")
sc = SparkContext.getOrCreate()
rdd = sc.textFile('<path-to-data>/bigdata.txt')
filter_empty_lines = contentRDD.filter(lambda x: len(x) > 0)
words = filter_empty_lines.flatMap(lambda x: x.split(' '))

wordcount = words.map(lambda x:(x,1)) \
.reduceByKey(lambda x, y: x + y) \
.map(lambda x: (x[1], x[0])).sortByKey(False)

for word in wordcount.collect():
    print(word)

wordcount.saveAsTextFile("/home/thiago/Wordcount")
