# Given the set representation, compute the minhash signatures of all documents using MapReduce.
# Minhash signatures: The H x N signature matrix: with H as the number of hash functions, N = 21,578

from pyspark import SparkConf, SparkContext
import time
import random
import math

def preprocess_data(rdd):
    rdd = rdd.map(lambda each_news_string: split_data(each_news_string))
    return rdd


def split_data(each_news_string):
    each_news_string = each_news_string[1:-1]
    each_news = each_news_string.split(", ")
    each_news = [int(characteristic) for characteristic in each_news]
    return each_news


# 產生hash function的係數：h(x) = ((ax + b) mod c) mod N
def generate_hash_coefficients(N):
    a = random.randint(1, N)
    b = random.randint(1, N)
    c = find_prime_larger_than(N)
    return [a, b, c, N]


def find_prime_larger_than(number):
    while True:
        number += 1
        if determine_if_prime(number) is True:
            return number


def determine_if_prime(number):
    for divisor in range(2, int(math.sqrt(number)) + 1):
        if number % divisor == 0:
            return False
    return True


def create_signatures(news_rdd, hash_coefficients):
    news_rdd = news_rdd.map(lambda each_news: min_hash(each_news, hash_coefficients))
    news = [news_rdd.collect()]
    return news


def min_hash(each_news, hash_coefficients):
    # 根據hash function轉換index
    hash_table = [index for index in range(N)]
    for index in range(N):
        each_news[index] = [hashing(index + 1, hash_coefficients, hash_table), each_news[index]]
    # 判斷出現1時最小的hash index
    for hash_index in range(N):
        if [hash_index, 1] in each_news:
            return hash_index


def hashing(x, coefficients, hash_table):
    [a, b, c, N] = coefficients
    hash_index = ((a * x + b) % c) % N
    if hash_index in hash_table:
        hash_table.remove(hash_index)
        return hash_index
    else:
        for index in hash_table:
            if index > hash_index:
                hash_index = index
                hash_table.remove(hash_index)
                return hash_index
        hash_index = hash_table[0]
        hash_table.remove(hash_index)
        return hash_index


if __name__ == "__main__":
    APP_NAME = "hw3_problem02"
    MASTER_URL = "local[*]"
    FILE_PATH = ".\\output01_201805021648\\part-00001"
    HASHING_H = 6

    # 記錄開始的時間
    start_time = time.time()

    # 初始化
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster(MASTER_URL)
    sc = SparkContext(conf = conf)

    rdd = preprocess_data(sc.textFile(FILE_PATH))

    N = len(rdd.take(1)[0])
    coefficients_list = [generate_hash_coefficients(N) for index in range(HASHING_H)]

    min_hash_rdd = sc.parallelize([])
    for coefficients in coefficients_list:
        min_hash_rdd += sc.parallelize(create_signatures(rdd, coefficients))

    min_hash_rdd = min_hash_rdd.coalesce(1)
    min_hash_rdd.saveAsTextFile("output02_{}".format(time.strftime("%Y%m%d%H%M", time.localtime())))

    # 記錄結束的時間
    print("Total running time: {}".format(time.time() - start_time))
