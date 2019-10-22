# Given the Reuters-21578 dataset, please calculate the term frequencies, and output the representation of the document contents as a term-document count matrix.
# term-document matrix:
# A M x N matrix: with rows as term frequencies and columns as documents (N = 21,578)

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


# 將[doc1.text, doc2.text, ...]
# 映射成[(doc1.text, doc1.index), (doc2.text, doc2.index), ...]
# 映射成[[((doc1.term, doc1.index), 1), ...], [((doc2.term, doc2.index), 1), ...], ...]
def create_terms(rdd):
    rdd = rdd.zipWithIndex()
    rdd = rdd.map(lambda pair: create_term(pair[0], pair[1]))
    return rdd


def create_term(document, news_index):
    document = document.split(" ")
    terms = [((term, news_index), 1) for term in document if term != ""]
    return terms


# 將[[((doc1.term, doc1.index), 1), ...], [((doc2.term, doc2.index), 1), ...], ...]
# 映射成[((doc1.term, doc1.index), 1), ..., ((doc2.term, doc2.index), 1), ...]
# 歸納成[((doc1.term, doc1.index), n), ..., ((doc2.term, doc2.index), n), ...]
# 映射成[(doc1.term, (doc1.index, n)), ..., (doc2.term, (doc2.index, n)), ...]
# 歸納成[(term, [(doc1.index, n), (doc2.index, n), ...]), (term, [(doc1.index, n), (doc2.index, n), ...]), ...]
def group_terms(rdd):
    rdd = rdd.flatMap(lambda terms: terms)
    rdd = rdd.reduceByKey(lambda a, b: a + b)
    rdd = rdd.map(lambda term: (term[0][0], (term[0][1], term[1])))
    rdd = rdd.reduceByKey(lambda a, b: combine(a, b))
    return rdd


def combine(a, b):
    if type(a[0]) is int and type(b[0]) is int:
        return [a, b]
    if type(a[0]) is not int and type(b[0]) is int:
        a.append(b)
        return a
    if type(a[0]) is int and type(b[0]) is not int:
        b.append(a)
        return b
    if type(a[0]) is not int and type(b[0]) is not int: 
        return a + b


def create_matrix(terms_document_rdd, N):
    matrix = terms_document_rdd.map(lambda term: create_matrix_row(term[1]))
    return matrix


def create_matrix_row(document_list):
    if type(document_list[0]) is int:
        return [0 if document_list[0] != index else document_list[1] for index in range(N)]
    else:
        matrix_row = []
        for index in range(N):
            if len(document_list) > 0:
                if document_list[0][0] == index:
                    matrix_row.append(document_list[0][1])
                    document_list.remove(document_list[0])
                else:
                    matrix_row.append(0)
            else:
                matrix_row.append(0)
        return matrix_row


if __name__ == "__main__":
    APP_NAME = "hw4_problem01"
    MASTER_URL = "spark://192.168.1.103:7077"
    HOME_PATH = "D:\\data\\"

    # 記錄開始的時間
    start_time = time.time()

    # 初始化
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster(MASTER_URL)
    sc = SparkContext(conf = conf)

    # 讀取檔案並進行預處理以取得每篇新聞的body.text
    document_rdd = preprocess_data(sc.wholeTextFiles(HOME_PATH + "reut2-[0-9]*.sgm"))
    # 計算每篇新聞的terms
    document_terms_rdd = create_terms(document_rdd)
    # 計算每個terms的新聞
    terms_document_rdd = group_terms(document_terms_rdd)

    N = document_terms_rdd.count()

    # 根據每個terms的新聞建立矩陣
    matrix_rdd = create_matrix(terms_document_rdd, N)
    # matrix_rdd = matrix_rdd.coalesce(1)

    terms_rdd = terms_document_rdd.map(lambda term: term[0])
    terms_rdd = terms_rdd.coalesce(1)
    terms_rdd = terms_rdd.union(matrix_rdd)
    terms_rdd.saveAsTextFile("D:\\output01_{}".format(time.strftime("%Y%m%d%H%M", time.localtime())))

    # 記錄結束的時間
    print("Total running time: {}".format(time.time() - start_time))
