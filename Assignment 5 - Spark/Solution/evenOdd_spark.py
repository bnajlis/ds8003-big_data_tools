from pyspark import SparkConf, SparkContext

def checkEvenODD(line):
    if int(line) % 2:
        return ("EVEN", 1)
    else:
        return ("ODD", 1)
def main(sc):
    textFile = sc.textFile("/user/root/integer_list")
    wordList = textFile.map(lambda line: checkEvenODD(line))
    wordsWithTotalCount = wordList.reduceByKey(lambda v1, v2: v1+v2)
    wordsWithTotalCount.saveAsTextFile("/user/root/spark_even_odd_count.txt")
    topK = wordsWithTotalCount.collect()
    print(topK)


if __name__  == "__main__":
    conf = SparkConf().setAppName("MyApp")
    sc = SparkContext(conf = conf)
    main(sc)
    sc.stop()



#spark-submit --master yarn-client --executor-memory 512m --num-executors 3 --executor-cores 1 --driver-memory 512m evenOdd.py