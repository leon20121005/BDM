# Given the Google web graph dataset, please output the list of web pages with the number of outlinks,
# sorted in descending order of the out-degrees.

# A sorted list of pages with their out-degrees. Each line contains: <NodeID>, <out-degree>

from pyspark import SparkConf, SparkContext
import time

APP_NAME = "hw5_problem01"
MASTER_URL = "local[*]"
HOME_PATH = ".\\"

def preprocess_data(rdd):
    rdd = rdd.zipWithIndex().filter(lambda row: row[1] >= 4)
    rdd = rdd.map(lambda row: tuple([int(node_id) for node_id in row[0].split()]))
    return rdd


if __name__ == "__main__":
    # 記錄開始的時間
    start_time = time.time()

    # 初始化
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster(MASTER_URL)
    sc = SparkContext(conf = conf)

    rdd = preprocess_data(sc.textFile(HOME_PATH + "web-Google.txt"))

    rdd = rdd.map(lambda pair: (pair[0], 1)).reduceByKey(lambda a, b: a + b)
    rdd = rdd.map(lambda pair: (pair[1], pair[0])).sortByKey(ascending = False).map(lambda pair: (pair[1], pair[0]))

    rdd.saveAsTextFile(".\\output01")

    # 記錄結束的時間
    print("Total running time: {}".format(time.time() - start_time))
