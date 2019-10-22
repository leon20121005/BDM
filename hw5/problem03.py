# Design an algorithm that maintains the connectivity of two nodes in an efficient way. Given a node v,
# please output the list of nodes that v points to, and the list of nodes that points to v.

# Given a node v, The first line contains a list of nodes that v points to: <ToNodeID>, …, <ToNodeID>
# The second line contains a list of nodes point to v <FromNodeID>, …, <FromNodeID>

from pyspark import SparkConf, SparkContext
import time

APP_NAME = "hw5_problem03"
MASTER_URL = "local[*]"
HOME_PATH = ".\\"

INPUT_NODE = 824020

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

    rdd = rdd.filter(lambda row: row[0] == INPUT_NODE or row[1] == INPUT_NODE)
    inverse_rdd = rdd.map(lambda row: (row[1], row[0]))

    to_node_rdd = rdd.groupByKey().filter(lambda row: row[0] == INPUT_NODE).map(lambda row: list(row[1]))
    from_node_rdd = inverse_rdd.groupByKey().filter(lambda row: row[0] == INPUT_NODE).map(lambda row: list(row[1]))

    to_node_list = to_node_rdd.collect()[0]
    from_node_list = from_node_rdd.collect()[0]

    print("Given the node {}, the list of nodes that {} points to:".format(INPUT_NODE, INPUT_NODE))
    print(to_node_list)
    print("The list of nodes point to {}".format(INPUT_NODE))
    print(from_node_list)

    # 記錄結束的時間
    print("Total running time: {}".format(time.time() - start_time))
