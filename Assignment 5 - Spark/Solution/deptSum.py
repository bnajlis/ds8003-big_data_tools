from pyspark import SparkConf, SparkContext

def makeTuple(line):
    words = line.split()
    return (words[0], int(words[1]))
def main(sc):
    textFile = sc.textFile("/user/root/dept_salary")
    wordList = textFile.map(lambda line: makeTuple(line))
    sumCount = wordList.reduceByKey(lambda x,y:  x +y)
    print sumCount.collectAsMap()
    sumCount.saveAsTextFile("/user/root/dept_sum.txt")


if __name__  == "__main__":
    conf = SparkConf().setAppName("MyApp")
    sc = SparkContext(conf = conf)
    main(sc)
    sc.stop()



#spark-submit --master yarn-client --executor-memory 512m --num-executors 3 --executor-cores 1 --driver-memory 512m deptAverage.py