# Given the set representation, compute the minhash signatures of all documents using MapReduce.
# Minhash signatures: The H x N signature matrix: with H as the number of hash functions, N = 21,578

from pyspark import SparkConf, SparkContext
import time
from bs4 import BeautifulSoup
import random
import math

def preprocess_data(rdd):
    rdd = rdd.flatMap(lambda pair: parse_document(pair))
    return rdd


def parse_document(pair):
    soup = BeautifulSoup(pair[1], "html.parser")
    body_list = soup.find_all("body")
    documents = [remove_noise(body.text) for body in body_list]
    return documents


def remove_noise(text):
    text = text.replace(".\n", " ")
    text = text.replace("\n", " ")
    text = text.replace(",", "")
    text = text.replace("Reuter", "")
    text = text.replace("\x03", "")
    return text


def create_shingles(rdd):
    rdd = rdd.zipWithIndex()
    rdd = rdd.map(lambda pair: create_shingle(pair[0], pair[1]))
    return rdd


def create_shingle(document, news_index):
    document = document.split(" ")
    document = [word for word in document if word != ""]
    shingle = []
    for index in range(len(document) - 4):
        temp_row = ""
        for k in range(SHINGLING_K):
            temp_row += "{} ".format(document[index + k])
        temp_row = temp_row[:-1]
        shingle.append([temp_row, news_index])
    return shingle


def group_shingles(rdd):
    rdd = rdd.flatMap(lambda shingle: shingle)
    rdd = rdd.groupByKey().map(lambda shingle: (shingle[0], list(shingle[1])))
    return rdd


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


def create_signatures_rdd(hash_coefficients, all_shingles_rdd):
    # 根據hash function轉換index
    original_indexes = [index for index in range(N)]
    hash_table = [index for index in range(N)]
    for index in range(N):
        original_indexes[index] = hashing(index + 1, hash_coefficients, hash_table)
    hashed_indexes = original_indexes
    # 根據hash index建立每篇新聞的signature
    signatures = []
    for news_index in range(M):
        signature = create_signature(hashed_indexes, news_index, all_shingles_rdd)
        signatures.append(signature)
    signatures_rdd = sc.parallelize([signatures])
    return signatures_rdd


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


def create_signature(hash_indexes, news_index, all_shingles_rdd):
    signature = find_min(hash_indexes, news_index, all_shingles_rdd)
    return signature


def find_min(hash_indexes, news_index, all_shingles_rdd):
    # 判斷出現1時最小的hash index
    for hash_index in range(N):
        original_index = hash_indexes.index(hash_index)
        print(news_index)
        print(all_shingles_rdd.lookup(original_index)[0][1])
        if news_index in all_shingles_rdd.lookup(original_index)[0][1]:
            return hash_index


if __name__ == "__main__":
    APP_NAME = "hw3_problem02"
    MASTER_URL = "local[*]"
    HOME_PATH = ".\\data\\"
    SHINGLING_K = 2
    HASHING_H = 6

    # 記錄開始的時間
    start_time = time.time()

    # 初始化
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster(MASTER_URL)
    sc = SparkContext(conf = conf)

    news_rdd = preprocess_data(sc.wholeTextFiles(HOME_PATH + "reut2-001.sgm" + "," + HOME_PATH + "reut2-002.sgm"))
    news_shingles_rdd = create_shingles(news_rdd)

    all_shingles_rdd = group_shingles(news_shingles_rdd)
    all_shingles_rdd = all_shingles_rdd.zipWithIndex().map(lambda each_shingle: [each_shingle[1], each_shingle[0]])

    N = all_shingles_rdd.count()
    M = news_shingles_rdd.count()

    coefficients_list = [generate_hash_coefficients(N) for index in range(HASHING_H)]

    min_hash_rdd = sc.parallelize([])
    for coefficients in coefficients_list:
        min_hash_rdd += create_signatures_rdd(coefficients, all_shingles_rdd)

    min_hash_rdd = min_hash_rdd.coalesce(1)
    min_hash_rdd.saveAsTextFile("output02_{}".format(time.strftime("%Y%m%d%H%M", time.localtime())))

    # 記錄結束的時間
    print("Total running time: {}".format(time.time() - start_time))
