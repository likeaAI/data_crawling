from selenium import webdriver
import time
import os
import shutil
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException

import jaydebeapi

if __name__ == "__main__":

    conn = jaydebeapi.connect(
        "com.tmax.tibero.jdbc.TbDriver",
        "jdbc:tibero:thin:@172.7.0.23:8629:tibero",
        ["labelon", "euclid!@)$labelon"],
        "tibero6-jdbc.jar",
    )
    cur = conn.cursor()
    # 크롬드라이버 다운로드 후, path 설정
    executable_path = r'C:/euclid/chromedriver/chromedriver.exe'
    # csv 파일 다운로드 위치
    current_file_path = 'C:/Users/yoonsub/Downloads'
    # csv 파일 이동 dir 위치
    destination_path = 'C:/euclid/nl2sql/ws'

    driver = webdriver.Chrome(executable_path=executable_path)

    # url = 'https://www.data.go.kr/tcs/dss/selectDataSetList.do?dType=FILE&keyword=&detailKeyword=&publicDataPk=&recmSe=&detailText=&relatedKeyword=&commaNotInData=&commaAndData=&commaOrData=&must_not=&tabId=&dataSetCoreTf=&coreDataNm=&sort=&relRadio=&orgFullName=&orgFilter=&org=&orgSearch=&currentPage=1&perPage=10&brm=&instt=&svcType=&kwrdArray=&extsn=CSV&coreDataNmArray=&pblonsipScopeCode='
    # csv 파일 다운로드 url
    # 공공데이터 포털 확장자 csv 선택 후 검색 -> 파일데이터 선택 -> 페이지 선택(현재 231) -> url 복사 후 설정
    url = 'https://www.data.go.kr/tcs/dss/selectDataSetList.do?dType=FILE&keyword=&detailKeyword=&publicDataPk=&recmSe=&detailText=&relatedKeyword=&commaNotInData=&commaAndData=&commaOrData=&must_not=&tabId=&dataSetCoreTf=&coreDataNm=&sort=&relRadio=&orgFullName=&orgFilter=&org=&orgSearch=&currentPage=300&perPage=10&brm=&instt=&svcType=&kwrdArray=&extsn=CSV&coreDataNmArray=&pblonsipScopeCode='

    # 279
    driver.get(url)

    count = 1
    # DATA_BASIC_INFO 테이블에서 max id 값 가져오기(insert 시 필요)
    max_id_sql = "select max(id) from TE_DATA_BASIC_INFO"
    cur.execute(max_id_sql) 
    max_id = cur.fetchone()[0] + 1
    while count < 5000:
        for j in range(3, 13):
            for i in range(1, 11):
                # 암묵적 대기 5s
                driver.implicitly_wait(5)
                try:
                    # 1.CSV 파일 다운로드
                    driver.find_element_by_xpath(f'//*[@id="fileDataList"]/div[2]/ul/li[{i}]/div[2]/a').send_keys(Keys.ENTER)
                    # 2.DATA_BASIC_INFO 테이블 필요 데이터 search
                    category = driver.find_element_by_xpath(f'//*[@id="fileDataList"]/div[2]/ul/li[{i}]/p/span[1]').text
                    desc = driver.find_element_by_xpath(f'//*[@id="fileDataList"]/div[2]/ul/li[{i}]/dl/dd').text.replace("'","")
                    name = driver.find_element_by_xpath(f'//*[@id="fileDataList"]/div[2]/ul/li[{i}]/dl/dt/a/span[@class="title"]').text
                    url = driver.find_element_by_xpath(f'//*[@id="fileDataList"]/div[2]/ul/li[{i}]/dl/dt/a').get_attribute('href')
                    key = url.split('/')[-2]
                    time.sleep(3)
                except NoSuchElementException:
                    continue
                
                # 3.다운로드한 데이터의 data_origin_key를 기존 DATA_BASIC_INFO 테이블 내 데이터와 비교
                inserted_check_sql = f"select id from TE_DATA_BASIC_INFO where data_origin_key='{key}'"
                cur.execute(inserted_check_sql)
                inserted_check = cur.fetchall()
                
                # DATA_BASIC_INFO 테이블에 data_origin_key가 없으면 insert
                if not inserted_check:
                    sql = f"INSERT INTO TE_DATA_BASIC_INFO(id, collect_site_id, category_big, category_small, data_name, data_description, provide_url_link, provide_data_type, collect_data_type, collect_url_link, is_collect_yn, data_origin_key) VALUES('{max_id}', 2, '공공데이터', '{category}', '{name}', '{desc}', '[{url}]', '[File]', '[File]', '[{url}]', 'N', '{key}')"
                    cur.execute(sql)
                    print('inserted')
                    
                    # 4.다운로드, insert가 완료된 파일을 1_1_data_public_read_csv.py에 사용 가능하도록 이동
                    time.sleep(6)
                    file_list = os.listdir(current_file_path)
                    for file in file_list:
                        shutil.move(f'{current_file_path}/{file}', destination_path) if file.startswith(name) else None

                print(f'count: {count}')
                count += 1
                max_id += 1
            # 페이지 이동
            driver.find_element_by_xpath(f'//*[@id="fileDataList"]/nav/a[{j}]').send_keys(Keys.ENTER)
            # 암묵적 대기 5s
            driver.implicitly_wait(20)
    cur.close()