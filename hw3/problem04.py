from pyspark import SparkConf, SparkContext
from bs4 import BeautifulSoup
import time
import random
import math
import numpy as np

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
    rdd = rdd.map(lambda news: create_shingle(news))
    return rdd


def create_shingle(document):
    document = document.split(" ")
    document = [word for word in document if word != ""]
    shingle = []
    for index in range(len(document) - 4):
        temp_row = ""
        for k in range(SHINGLING_K):
            temp_row += "{} ".format(document[index + k])
        temp_row = temp_row[:-1]
        shingle.append(temp_row)
    return shingle


def get_target_news(doc_one_index, doc_two_index, news_rdd):
    news_rdd = news_rdd.zipWithIndex()
    news_rdd = news_rdd.filter(lambda news: news[1] == doc_one_index or news[1] == doc_two_index)
    news_rdd = news_rdd.map(lambda news: news[0])
    return news_rdd


def linear_search(news_shingles_rdd):
    shingles_rdd = news_shingles_rdd.flatMap(lambda each_news_shingles: each_news_shingles)
    N = shingles_rdd.count()
    shingles_rdd = shingles_rdd.map(lambda shingle: (shingle, 1))
    reduced_shingles_rdd = shingles_rdd.reduceByKey(lambda a, b: a + b)
    same_shingles_rdd = reduced_shingles_rdd.filter(lambda shingle: shingle[1] != 1)
    same_shingles_count = same_shingles_rdd.count()
    return same_shingles_count / N


def look_up_table(doc_one_index, doc_two_index, table_rdd):
    look_up_result = table_rdd.filter(lambda element: element == (doc_one_index, doc_two_index))
    if look_up_result.count() == 0:
        return False
    else:
        return True


if __name__ == "__main__":
    APP_NAME = "hw3_problem04"
    MASTER_URL = "spark://192.168.1.103:7077"
    HOME_PATH = "D:\\data\\"
    SHINGLING_K = 1
    DOCUMENT_ONE_INDEX = 0
    DOCUMENT_TWO_INDEX = 3

    # 初始化
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster(MASTER_URL)
    sc = SparkContext(conf = conf)

    # 讀取檔案並進行預處理以取得每篇新聞的body.text
    news_rdd = preprocess_data(sc.wholeTextFiles(HOME_PATH + "reut2-[0-9]*.sgm"))
    # 取得要比較的兩篇新聞的body.text
    news_rdd = get_target_news(DOCUMENT_ONE_INDEX, DOCUMENT_TWO_INDEX, news_rdd)
    # 計算兩篇新聞的shingles
    news_shingles_rdd = create_shingles(news_rdd)

    # 執行linear search
    start_time = time.time()
    linear_search_result = linear_search(news_shingles_rdd)
    linear_search_time = time.time() - start_time

    # 讀取檔案並進行預處理以取得所有的candidate pair
    table_rdd = sc.textFile("D:\\output03_201805060145\\part-00000")
    table_rdd = table_rdd.map(lambda x: [int(i) for i in x[1:-1].split(", ")])

    # 執行k-NN search
    start_time = time.time()
    knn_search_result = look_up_table(0, 3, table_rdd)
    knn_search_time = time.time() - start_time

    # 顯示執行結果
    print("Comparison between document {} and document {}".format(DOCUMENT_ONE_INDEX, DOCUMENT_TWO_INDEX))
    print("Linear search similarity: {}".format(linear_search_result))
    print("k-NN search:")
    if knn_search_result:
        print("yes")
    else:
        print("no")

    # 顯示執行時間差異
    print("Linear search running time: {}".format(linear_search_time))
    print("k-NN search running time: {}".format(knn_search_time))
