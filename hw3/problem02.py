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


def create_set_representation(shingles_news_rdd, N):
    set_representation = shingles_news_rdd.map(lambda shingle: [1 if index in shingle[1] else 0 for index in range(N)])
    return set_representation


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


def hashing(row, index, coefficients):
    [a, b, c, N] = coefficients
    hash_index = ((a * index + b) % c) % N
    for document_index in range(len(row)):
        row[document_index] = [row[document_index], hash_index]
    return row


# 對每一列[[news1.characteristic, hash_index], [news2.characteristic, hash_index], ...]進行歸納
# 先判斷上一列的characteristic，如果為0則直接將hash_index設為無限大(一定會被下一列characteristic為1的取代)
# 如果下一列的characteristic為1且hash_index小於上一列的hash_index則取代上一列
def reduce_hash_index(a, b):
    for document_index in range(len(a)):
        a_characteristic = a[document_index][0]
        b_characteristic = b[document_index][0]
        a_hash_index = a[document_index][1]
        b_hash_index = b[document_index][1]
        if a_characteristic == 0:
            a_hash_index = INFINITY
        if b_hash_index < a_hash_index and b_characteristic == 1:
            a[document_index][0] = b[document_index][0]
            a[document_index][1] = b[document_index][1]
    return a


if __name__ == "__main__":
    APP_NAME = "hw3_problem02"
    MASTER_URL = "spark://192.168.1.103:7077"
    HOME_PATH = "D:\\data\\"
    SHINGLING_K = 2
    HASHING_H = 2
    INFINITY = 99999999

    # 記錄開始的時間
    start_time = time.time()

    # 初始化
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster(MASTER_URL)
    sc = SparkContext(conf = conf)

    # 讀取檔案並進行預處理以取得每篇新聞的body.text
    news_rdd = preprocess_data(sc.wholeTextFiles(HOME_PATH + "reut2-[0-9]*.sgm"))

    # 計算每篇新聞的shingles
    news_shingles_rdd = create_shingles(news_rdd)
    # 計算每個shingle的新聞
    shingles_news_rdd = group_shingles(news_shingles_rdd)

    N = news_shingles_rdd.count()

    # 根據每個shingle的新聞建立矩陣
    set_representation_rdd = create_set_representation(shingles_news_rdd, N)
    set_representation_rdd = set_representation_rdd.coalesce(1)

    coefficients_list = [generate_hash_coefficients(N) for index in range(HASHING_H)]

    set_representation_rdd = set_representation_rdd.zipWithIndex()
    min_hash_rdd = sc.parallelize([])
    for coefficients in coefficients_list:
        hash_rdd = set_representation_rdd.map(lambda x: hashing(x[0], x[1] + 1, coefficients))
        each_min_hash_rdd = sc.parallelize(hash_rdd.reduce(lambda a, b: reduce_hash_index(a, b)))
        each_min_hash_rdd = sc.parallelize([each_min_hash_rdd.map(lambda x: x[1]).collect()])
        min_hash_rdd += each_min_hash_rdd

    min_hash_rdd = min_hash_rdd.coalesce(1)
    min_hash_rdd.saveAsTextFile("D:\\output02_{}".format(time.strftime("%Y%m%d%H%M", time.localtime())))

    # 記錄結束的時間
    print("Total running time: {}".format(time.time() - start_time))
