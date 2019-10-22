# Implement matrix multiplication by MapReduce. Your program should be able to output the result in appropriate dimensions.
# result of matrix multiplication:
# An M x R matrix: (M x N) * (N x R)

from pyspark import SparkConf, SparkContext
import time

def map_matrix(rdd1, rdd2):
    rdd1 = rdd1.zipWithIndex()
    rdd1 = rdd1.flatMap(lambda row: map_first_matrix(row[0], row[1]))
    rdd2 = rdd2.zipWithIndex()
    rdd2 = rdd2.flatMap(lambda row: map_second_matrix(row[0], row[1]))
    rdd = rdd1.union(rdd2)
    return rdd


def map_first_matrix(row, current_row):
    temporary_row = []
    for column_index in range(R):
        temporary_row += [((current_row + 1, column_index + 1, index + 1), row[index]) for index in range(len(row))]
    return temporary_row


def map_second_matrix(row, current_row):
    temporary_row = []
    for row_index in range(M):
        temporary_row += [((row_index + 1, index + 1, current_row + 1), row[index]) for index in range(len(row))]
    return temporary_row


def reduce_matrix(rdd):
    rdd = rdd.reduceByKey(lambda a, b: a * b)
    rdd = rdd.map(lambda x: ((x[0][0], x[0][1]), x[1]))
    rdd = rdd.reduceByKey(lambda a, b: a + b)
    return rdd


def convert_matrix(rdd):
    rdd = rdd.sortByKey(ascending = True)
    rdd = rdd.map(lambda x: (x[0][0], x[1]))
    rdd = rdd.groupByKey().map(lambda x: list(x[1]))
    return rdd


if __name__ == "__main__":
    APP_NAME = "hw4_problem02"
    MASTER_URL = "spark://192.168.1.103:7077"

    # 記錄開始的時間
    start_time = time.time()

    # 初始化
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster(MASTER_URL)
    sc = SparkContext(conf = conf)

    matrix1_rdd = sc.parallelize([[1, 2, 3], [4, 5, 0], [7, 8, 9], [10, 11, 12]])
    matrix2_rdd = sc.parallelize([[10, 15], [0, 2], [11, 9]])

    M = matrix1_rdd.count()
    N = matrix2_rdd.count()
    R = len(matrix2_rdd.first())

    rdd = map_matrix(matrix1_rdd, matrix2_rdd)
    rdd = reduce_matrix(rdd)
    matrix_rdd = convert_matrix(rdd)

    # 顯示結果
    matrix1 = matrix1_rdd.collect()
    matrix2 = matrix2_rdd.collect()
    matrix = matrix_rdd.collect()
    print("The first matrix ({} x {})".format(M, N))
    for row in matrix1:
        print(row)
    print("The second matrix ({} x {})".format(N, R))
    for row in matrix2:
        print(row)
    print("The result matrix of multiplication ({} x {})".format(M, R))
    for row in matrix:
        print(row)

    # 記錄結束的時間
    print("Total running time: {}".format(time.time() - start_time))
