# Given the Reuters-21578 dataset, please calculate all k-shingles and output the set representation of the text dataset as a matrix.
# Set representation: A M x N matrix: with rows as shingles and columns as documents (N = 21,578)

from pyspark import SparkConf, SparkContext
import time
from bs4 import BeautifulSoup

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


# 將[news1.text, news2.text, ...]
# 映射成[[news1.text, news1.index], [news2.text, news2.index], ...]
# 映射成[[[news1.shingle, news1.index], ...], [[news2.shingle, news2.index], ...], ...]
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


# 將[[[news1.shingle, news1.index], ...], [[news2.shingle, news2.index], ...], ...]
# 映射成[[news1.shingle, news1.index], ..., [news2.shingle, news2.index], ...]
# 並根據shingle合併shingle一樣的news.index
def group_shingles(rdd):
    rdd = rdd.flatMap(lambda shingle: shingle)
    rdd = rdd.groupByKey().map(lambda shingle: (shingle[0], list(shingle[1])))
    return rdd


def create_set_representation(shingles_news_rdd, N):
    set_representation = shingles_news_rdd.map(lambda shingle: [1 if index in shingle[1] else 0 for index in range(N)])
    return set_representation


if __name__ == "__main__":
    APP_NAME = "hw3_problem01"
    MASTER_URL = "spark://192.168.1.103:7077"
    HOME_PATH = "D:\\data\\"
    SHINGLING_K = 2

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

    shingles_rdd = shingles_news_rdd.map(lambda shingles_news: shingles_news[0])
    shingles_rdd = shingles_rdd.coalesce(1)
    shingles_rdd = shingles_rdd.union(set_representation_rdd)
    shingles_rdd.saveAsTextFile("D:\\output01_{}".format(time.strftime("%Y%m%d%H%M", time.localtime())))

    # 記錄結束的時間
    print("Total running time: {}".format(time.time() - start_time))
