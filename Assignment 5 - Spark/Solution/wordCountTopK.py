from pyspark import SparkConf, SparkContext


def main(sc):
    textFile = sc.textFile("shakespeare_100.txt")
    wordList = textFile.flatMap(lambda line: line.split())
    wordCount = wordList.map(lambda word: (word,1))
    wordsWithTotalCount = wordCount.reduceByKey(lambda v1, v2: v1+v2)
    topKDesc = wordsWithTotalCount.top(10, key=lambda x:x[1])
    topKAsc = wordsWithTotalCount.takeOrdered(10, key=lambda x:x[1])
    print(topKDesc)
    print(topKAsc)

if __name__  == "__main__":
    conf = SparkConf().setAppName("MyApp")
    sc = SparkContext(conf = conf)
    main(sc)
    sc.stop()



#spark-submit --master yarn-client --executor-memory 512m --num-executors 3 --executor-cores 1 --driver-memory 512m wordCountTopK.py