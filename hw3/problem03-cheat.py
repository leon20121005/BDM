from pyspark import SparkConf, SparkContext
import time
import random
import math
import numpy as np

def LSH(m_transpose):
    m_transpose = m_transpose.zipWithIndex()
    band = m_transpose.map(lambda row: (hash(str(row[0])), row[1]))
    band_group = band.groupByKey().map(lambda x: list(x[1]))
    band_group = band_group.filter(lambda row: len(row) != 1)
    pair_list = band_group.map(lambda x: pair(x))
    return(pair_list)

def pair(row):
    pair_list = []
    for x_index in range(len(row)):
        for y_index in range(x_index + 1, len(row)):
            pair_list.append((row[x_index], row[y_index]))
    return pair_list

if __name__ == "__main__":
    APP_NAME = "hw3_problem03"
    MASTER_URL = "local[*]"
    HOME_PATH = ".\\output02_201805061925\\"

    # 記錄開始的時間
    start_time = time.time()

    # 初始化
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster(MASTER_URL)
    sc = SparkContext(conf = conf)

    rdd = sc.textFile(HOME_PATH + "part-00000")
    rdd = rdd.map(lambda x: [int(i) for i in x[1:-1].split(", ")])

    test = rdd.collect()

    transposed = [[test[0][index], test[1][index]] for index in range(len(test[0]))]

    rdd = sc.parallelize(transposed)
    result = LSH(rdd)
    pair_set = result.flatMap(lambda x: x).distinct()

    pair_set = pair_set.coalesce(1)
    pair_set.saveAsTextFile("output03_{}".format(time.strftime("%Y%m%d%H%M", time.localtime())))

    # 記錄結束的時間
    print("Total running time: {}".format(time.time() - start_time))
