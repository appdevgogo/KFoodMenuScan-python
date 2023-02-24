import configparser
import pymysql
import time
import datetime
import urllib.request
import requests
import csv
import wikipediaapi

from bs4 import BeautifulSoup
from urllib import parse
from urllib.request import urlopen
from googletrans import Translator

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from google.cloud import translate_v2 as translate
#from google.cloud import translate_v3beta1 as translate
import os


def init():

    global start_time, url, key, \
        aws_host, aws_user, aws_pw, aws_dbname, aws_db, aws_cursor, \
        client_id, client_secret

    start_time = time.time()
    print(datetime.datetime.now(), "작업시작", "진행중..")

    config = configparser.ConfigParser()
    config.read('../0_config/kfood_config_220716.ini')

    url = config['DATAGOKR']['ENDPOINT']
    key_d = config['DATAGOKR']['KEY']
    key = parse.quote(key_d, safe='')  # s3 config.ini 에 저장된 decoding key값을 encoding 하는 문장

    aws_host = config['AWSRDS']['HOST']
    aws_user = config['AWSRDS']['USER']
    aws_pw = config['AWSRDS']['PW']
    aws_dbname = config['AWSRDS']['DB']

    client_id = config['NAVERSEARCH']['CLIENTID']
    client_secret = config['NAVERSEARCH']['CLIENTSECRET']

    aws_db = pymysql.connect(
        host=aws_host,
        port=3306,
        user=aws_user,
        passwd=aws_pw,
        db=aws_dbname,
        charset='utf8'
    )

    aws_cursor = aws_db.cursor()

    #step_one()
    #step_two()
    #step_three()
    #step_four()
    #step_five()
    step_six()

    aws_cursor.close()
    aws_db.close()

    print("--- 누적 %s 초 소요됨. ---" % (time.time() - start_time), "작업 종료!!!!")

def step_one():

    start_time_step = time.time()

    del_table("kfood_info", "kfood_info")

    sql = "CREATE TABLE kfood_info " \
          "(" \
          "fdcode VARCHAR(10) NOT NULL, " \
          "fdnamekr VARCHAR(50) NOT NULL, " \
          "fdnameenp VARCHAR(50) NOT NULL, " \
          "fdnameen VARCHAR(100) NOT NULL, " \
          "fddescript VARCHAR(500) NOT NULL, " \
          "fdnamevcurl VARCHAR(500) NOT NULL, " \
          "fdpicurl VARCHAR(500) NOT NULL," \
          "fdingts VARCHAR(500) NOT NULL, " \
          "PRIMARY KEY (fdcode, fdnamekr)" \
          ")"

    aws_cursor.execute(sql)

    print("kfood_info 테이블 생성함")

    pageno = 1
    pagesize = 20
    params = "&Page_No=%d&Page_Size=%d" % (pageno, pagesize)
    datatype = "&service_Type=xml"
    open_url = url + key + params + datatype

    res = requests.get(open_url)
    soup = BeautifulSoup(res.content, 'xml')
    totalcount = soup.find_all('total_Count')

    print(totalcount)

    # 만약 한국음식 개수가 1266개 이면 총 64페이지가 발생됨 왜냐하면 1페이지당 20개까지만 추출 가능
    # 파이썬 range(1, 3) 이면 1,2만 실행됨
    count = int(int(totalcount[0].text) / pagesize) + 2

    for i in range(1, count):
        print("page no -->")
        print(i)
        pageno = i
        params = "&Page_No=%d&Page_Size=%d" % (pageno, pagesize)
        open_url = url + key + params + datatype

        res = requests.get(open_url)
        soup = BeautifulSoup(res.content, 'xml')
        data = soup.find_all('item')

        array_temp = []

        for item in data:
            fdcode = item.find('fd_Code').text
            fdnamekr = item.find('fd_Nm').text
            fdnameenp = ""
            fdnameen = ""
            fddescript = ""
            fdnamevcurl = ""
            fdpicurl = item.find('food_Image_Address').text
            #ingts = ""


            ingts = item.find_all('fd_Eng_Nm')
            array_temp_ingts = []

            for sitem in ingts:
                array_temp_ingts.append(sitem.text)

            #print(array_temp_ingts)

            ingts_str = ' // '.join(array_temp_ingts)

            array_temp.append([fdcode, fdnamekr, fdnameenp, fdnameen, fddescript, fdnamevcurl, fdpicurl, ingts_str])

        print(array_temp)

        sql = "INSERT INTO kfood_info " \
              "(fdcode, fdnamekr, fdnameenp, fdnameen, fddescript, fdnamevcurl, fdpicurl, fdingts) " \
              "VALUES " \
              "(%s, %s, %s, %s, %s, %s, %s, %s)"
        
        aws_cursor.executemany(sql, array_temp)
        aws_db.commit()

    print("--- 1단계 공공데이터 추출후 테이블 생성 및 입력 %s 초 소요됨. 작업완료! ---" % (time.time() - start_time_step))
    print("--- 누적 %s 초 소요됨. ---" % (time.time() - start_time), "다음작업 진행중..")

# 한국국제교류재단 엑셀자료 AWS RDS에 저장후 Join 하기
def step_two():

    start_time_step = time.time()

    del_table("kfood_info", "kfood_info_add")

    sql = "CREATE TABLE kfood_info_add " \
          "(" \
          "fdcode VARCHAR(10) NOT NULL, " \
          "fdnamekr VARCHAR(50) NOT NULL, " \
          "fdnameen VARCHAR(100) NOT NULL, " \
          "fddescript VARCHAR(500) NOT NULL, " \
          "PRIMARY KEY (fdcode, fdnamekr)" \
          ")"

    aws_cursor.execute(sql)

    f = open("한국국제교류재단_한국음식정보_영어_20191227_modify.csv", 'r')
    food_add_array = csv.reader(f)

    print("----------------------->>rea")
    print(food_add_array)

    sql = "INSERT INTO kfood_info_add " \
          "(fdcode, fdnamekr, fdnameen, fddescript) " \
          "VALUES " \
          "(%s, %s, %s, %s)"

    aws_cursor.executemany(sql, food_add_array)
    aws_db.commit()

    sql_join = "UPDATE kfood_info A " \
            "LEFT JOIN kfood_info_add B " \
            "ON A.fdnamekr = B.fdnamekr " \
            "SET A.fdnameen = B.fdnameen"

    aws_cursor.execute(sql_join)
    aws_db.commit()

    sql_join = "UPDATE kfood_info A " \
            "LEFT JOIN kfood_info_add B " \
            "ON A.fdnamekr = B.fdnamekr " \
            "SET A.fddescript = B.fddescript"

    aws_cursor.execute(sql_join)
    aws_db.commit()

    print("--- 2단계 한국국제교류재단_한국음식정보_영어 테이블 생성 및 병합 %s 초 소요됨. 작업완료! ---" % (time.time() - start_time_step))
    print("--- 누적 %s 초 소요됨. ---" % (time.time() - start_time), "다음작업 진행중..")

def step_three():

    start_time_step = time.time()

    sql_join = "SELECT B.fdcode, B.fdnamekr, B.fdnameen, B.fddescript " \
            "FROM kfood_info A RIGHT JOIN kfood_info_add B " \
            "ON A.fdnamekr = B.fdnamekr " \
            "WHERE A.fdnamekr IS NULL"

    aws_cursor.execute(sql_join)
    insert_array = list(aws_cursor.fetchall())

    print(len(insert_array))
    print(insert_array)

    sql = "INSERT INTO kfood_info " \
          "(fdcode, fdnamekr, fdnameen, fddescript) " \
          "VALUES " \
          "(%s, %s, %s, %s)"

    aws_cursor.executemany(sql, insert_array)
    aws_db.commit()

    aws_db.commit()

    print("--- 3단계 한국국제교류재단_한국음식정보_영어 음식추가 %s 초 소요됨. 작업완료! ---" % (time.time() - start_time_step))
    print("--- 누적 %s 초 소요됨. ---" % (time.time() - start_time), "다음작업 진행중..")

def step_four():

    start_time_step = time.time()

    translator = Translator()

    sql = "SELECT fdcode, fdnamekr FROM kfood_info WHERE fdnameenp=''"

    aws_cursor.execute(sql)
    res = aws_cursor.fetchall()

    food_pro_array = []

    for item in res:
        str = translator.translate(item[1], src='ko', dest='ko').pronunciation
        food_pro_array.append(item + (str,))
        print(item[0], item[1], str)

        sql = "UPDATE kfood_info SET fdnameenp='%s' WHERE fdcode='%s'" % (str, item[0])

        aws_cursor.execute(sql)
        aws_db.commit()


    '''
    # google translation v2
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'celtic-defender-356715-7381bc0e1dcf.json'

    client = translate.Client()
    result = client.translate('안녕하세요', source_language='ko', target_language='en')
    print(result['translatedText'])
    '''
    '''
    #google translation v3
    client = translate.TranslationServiceClient()

    response = client.translate_text(
        parent=parent,
        contents=["안녕하세요"],
        mime_type='text/plain',  # mime types: text/plain, text/html
        source_language_code='ko',
        target_language_code='ko')

    for translation in response.translations:
        print('Translated Text: {}'.format(translation))
    '''


    '''
    del_table("kfood_info", "kfood_info_add_pro")

    sql = "CREATE TABLE kfood_info_add_pro " \
          "(" \
          "fdcode VARCHAR(10) NOT NULL, " \
          "fdnamekr VARCHAR(50) NOT NULL, " \
          "fdnameenp VARCHAR(50) NOT NULL, " \
          "PRIMARY KEY (fdcode, fdnamekr)" \
          ")"

    aws_cursor.execute(sql)

    sql = "INSERT INTO kfood_info_add_pro " \
          "(fdcode, fdnamekr, fdnameenp) " \
          "VALUES " \
          "(%s, %s, %s)"

    aws_cursor.executemany(sql, food_pro_array)
    aws_db.commit()

    sql_join = "UPDATE kfood_info A " \
            "LEFT JOIN kfood_info_add_pro B " \
            "ON A.fdcode = B.fdcode " \
            "SET A.fdnameenp = B.fdnameenp"

    aws_cursor.execute(sql_join)
    aws_db.commit()
    '''
    print("--- 4단계 영어발음형태 문자 추출 %s 초 소요됨. 작업완료! ---" % (time.time() - start_time_step))
    print("--- 누적 %s 초 소요됨. ---" % (time.time() - start_time), "다음작업 진행중..")

def step_five():

    #sql = "SELECT fdcode, fdnamekr FROM kfood_info WHERE fdpicurl=''"
    sql = "SELECT fdcode, fdnamekr FROM kfood_info"

    aws_cursor.execute(sql)
    res = aws_cursor.fetchall()

    for item in res:

        print(item[1])

        img_url = get_wiki_img_url(item[1])
        print(item[0], item[1], img_url)

        sql = "UPDATE kfood_info SET fdpicurl='%s' WHERE fdcode='%s'" % (img_url, item[0])

        aws_cursor.execute(sql)
        aws_db.commit()

def step_six():

    sql = "SELECT fdcode, fdnamekr FROM kfood_info WHERE fdpicurl='none' "

    aws_cursor.execute(sql)
    res = aws_cursor.fetchall()

    for item in res:
        print(item[0], item[1])

        img_url = get_naver_img_curl(item[1])

        print(item[0], item[1], img_url)

        sql = "UPDATE kfood_info SET fdpicurl='%s' WHERE fdcode='%s'" % (img_url, item[0])

        aws_cursor.execute(sql)
        aws_db.commit()

#----- 아래는 기본 함수들 정리 ------#
# 테이블 삭제 함수
def del_table(dbname, tablename):
    # 해당 이름의 테이블이 있는지 없는지 확인하는 것
    sql = "SELECT count(*) FROM Information_schema.tables WHERE table_schema = '%s' AND table_name = '%s'" % (dbname, tablename)

    aws_cursor.execute(sql)
    result = aws_cursor.fetchall()

    if 1 == result[0][0]:
        sql = "DROP TABLE %s" % (tablename)
        aws_cursor.execute(sql)
        print("%s 테이블 삭제함" % (tablename))

def get_wiki_img_url(name):

    wiki_wiki = wikipediaapi.Wikipedia('ko')
    page_py = wiki_wiki.page(name)

    if page_py.exists():
        url = page_py.fullurl
        html = urlopen(url)
        soup = BeautifulSoup(html, "html.parser")
        # html_tag = soup.select_one('meta[property="og:image"]')['content']
        # html_tag = soup.find("meta", property="og:image")["content"]
        html_tag = soup.find_all("meta", property="og:image")

        if len(html_tag) > 0:
            result = html_tag[-1].attrs["content"]

        else:
            result = "none"

    else:
        result = "none"

    return result

def get_naver_img_url(stxt):

    encText = urllib.parse.quote(stxt)

    naver_img_search_url = "https://openapi.naver.com/v1/search/image.xml?query=" + encText + "&filter=medium" # XML 결과
    #naver_img_search_url = "https://openapi.naver.com/v1/search/image?query=" + encText + "&filter=small" # JSON 결과

    request = urllib.request.Request(naver_img_search_url)
    request.add_header("X-Naver-Client-Id", client_id)
    request.add_header("X-Naver-Client-Secret", client_secret)

    response = urllib.request.urlopen(request)
    rescode = response.getcode()

    if (rescode == 200):
        res = response.read()
       # print(res.decode('utf-8'))

        soup = BeautifulSoup(res, 'xml')
        data = soup.find_all('link')

        if len(data) > 1:
            temp_data = data[1]
            result = temp_data.text
            print(temp_data.text)
        else:
            result = "none"

    else:
        print("Error Code:" + rescode)
        result = "none"

    return result

def get_naver_img_curl(stxt):

    encText = urllib.parse.quote(stxt)

    #//*[@id="main_pack"]/section[2]/div/div[1]/div[1]/div[1]/div/div[1]/a/img
    url_start = "https://search.naver.com/search.naver?where=image&section=image&query="
    url_end = "&res_fr=0&res_to=0&sm=tab_opt&color=&ccl=2&nso=so%3Ar%2Ca%3Aall%2Cp%3Aall&recent=0&datetype=0&startdate=0&enddate=0&gif=0&optStr=&nso_open=1&pq="

    naver_img_search_url = url_start + encText + url_end

    chrome_options = Options()
    chrome_options.add_argument('headless')
    chrome_options.add_argument('--log-level=3')
    chrome_options.add_argument('--disable-logging')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-gpu')

    driver = webdriver.Chrome(options=chrome_options)
    driver.get(naver_img_search_url)
    time.sleep(2)

    element = driver.find_element(By.XPATH, '//*[@id="main_pack"]/section[2]/div/div[1]/div[1]/div[1]/div/div[1]/a/img')

    result = str(element.get_attribute('src'))

    return result


init()
