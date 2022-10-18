import pickle
import re
import time
import requests
import googletrans
import jaydebeapi

# 문자열 중복제거
from collections import OrderedDict

# 숫자 -> 영어단어 변환 라이브러리
from num2words import num2words

# 파파고 api (구글번역라이브러가 잘 작동안될때 바꿔서 사용)
papago_client_id = 'HkxMHW9hezSa69Iwm2BQ'
papago_clientSecret = '1ZgqWsFhuS'

if __name__ == '__main__':

    # DB connection
    conn = jaydebeapi.connect(
        "com.tmax.tibero.jdbc.TbDriver",
        "jdbc:tibero:thin:@172.7.0.23:8629:tibero",
        ["labelon", "euclid!@)$labelon"],
        "tibero6-jdbc.jar",
    )

    cur = conn.cursor()
    print("LABELON_DB접속")

    # translator(구글번역)
    translator = googletrans.Translator()

    # MANAGE_PHYSICAL_TABLE select sql
    select_logical_table_korean  = "select ID, LOGICAL_TABLE_KOREAN as kor from TE_MANAGE_PHYSICAL_TABLE where TE_DATA_BASIC_ID in (select id from TE_DATA_BASIC_INFO where COLLECT_SITE_ID=2 and IS_COLLECT_YN='Y') and logical_table_english like 'DATA_TMP_%'"
    select_logical_table_korean2 = "SELECT id , LOGICAL_TABLE_KOREAN AS kor From TE_MANAGE_PHYSICAL_TABLE WHERE LOGICAL_TABLE_ENGLISH IS NULL"

    select_logical_table_korean3 = "SELECT id , LOGICAL_TABLE_KOREAN AS kor From TE_MANAGE_PHYSICAL_TABLE WHERE CHAR_LENGTH(LOGICAL_TABLE_ENGLISH) < = 10 "  # 구글번역이 씹히는경우 영어글자수로 10이하 검색 파파고로 재번역
    select_logical_table_korean4 = "SELECT id , LOGICAL_TABLE_KOREAN AS kor From TE_MANAGE_PHYSICAL_TABLE WHERE id = 4446 "  # 특정 아이디
    select_logical_table_korean5 = "select ID, LOGICAL_TABLE_KOREAN as kor from TE_MANAGE_PHYSICAL_TABLE where logical_table_english like 'DATA_TMP_%'"
    select_logical_table_korean6 = "select ID, LOGICAL_TABLE_KOREAN as kor from TE_MANAGE_PHYSICAL_TABLE where logical_table_english IS NULL"

    # 수정용 쿼리
    select_logical_table_modify = "select ID, LOGICAL_TABLE_ENGLISH as en from  TE_MANAGE_PHYSICAL_TABLE where logical_table_english not like 'DATA_TMP_%'"  # 특정 문자포함 재수정

    # MANAGE_PHYSICAL_COLUMN select sql
    select_logical_column_korean = "select ID, LOGICAL_COLUMN_KOREAN as kor FROM TE_MANAGE_PHYSICAL_COLUMN WHERE DATA_PHYSICAL_ID IN (SELECT ID FROM MANAGE_PHYSICAL_TABLE WHERE DATA_BASIC_ID IN (SELECT ID FROM DATA_BASIC_INFO WHERE COLLECT_SITE_ID='2')) AND LOGICAL_COLUMN_ENGLISH LIKE 'DATA_COL_%'"
    select_logical_column_korean2 = "select ID, LOGICAL_COLUMN_KOREAN as kor FROM TE_MANAGE_PHYSICAL_COLUMN WHERE LOGICAL_COLUMN_ENGLISH LIKE 'DATA_COL_%'"
    select_logical_column_korean3 = "select ID, LOGICAL_COLUMN_KOREAN as kor FROM TE_MANAGE_PHYSICAL_COLUMN WHERE LOGICAL_COLUMN_ENGLISH LIKE 'NL_DATA'"
    select_logical_column_korean4 = "select ID, LOGICAL_COLUMN_KOREAN as kor FROM TE_MANAGE_PHYSICAL_COLUMN WHERE LOGICAL_COLUMN_ENGLISH IS NULL"


    # 전처리 전치사(수정x , 수정시 약어처리된 회사명 다 틀어짐 )
    delete_words = [' that ', ' with ', ' amount ', ' about ', ' and ', ' the ', ' as ', ' of ',
                    ' by ', ' on ', ' for ', ' and ', ' an ', ' at ']

    # 변경,삭제단어, 사전 객체정보 불러와 사용
    with open("C:/Users/yoonsub/dict_data.pkl", 'rb') as f:
        dict_refine_words = pickle.load(f)


    # 파파고 번역함수
    def papago_translate(text):
        data = {'text': text,
                'source': 'ko',
                'target': 'en'}

        url = "https://openapi.naver.com/v1/papago/n2mt"

        header = {"X-Naver-Client-Id": papago_client_id,
                  "X-Naver-Client-Secret": papago_clientSecret}
        response = requests.post(url, headers=header, data=data)
        rescode = response.status_code

        if (rescode == 200):
            t_data = response.json()
            trans_data_papa = t_data['message']['result']['translatedText']
        else:
            print("Error Code:", rescode)
        return trans_data_papa



    def numtowords(en): # 맨앞단 숫자면 앞에 NO를 붙임
        en = en.split(' ')
        try:
            en_first = en[0][0]
            if en_first.isnumeric() == True :
                add_keyword = 'NO'
                en = " ".join(en)
                en = add_keyword + " " + en
                return en

            else:  # 숫자가 아닌경우 except로 보냄
                num2words(en[0][0])


        except:
            en = ' '.join(en)
            return en

    # 데이터를 select, 번역 후 각 테이블에 LOGICAL_*_ENGLISH update
    def name_translator_table(select_sql):
        time.sleep(0.2)
        print("####################")
        cur.execute(select_sql)
        select_data = cur.fetchall()
        print('selected')
        count = 1
        for data in select_data:

            id_ = data[0]
            print(f'id: {id_}')
            kor = data[1].replace('_', ' ')
            print(f'kor: {kor}')
            result = translator.translate(kor, dest='en') #1 파파고 사용시 주석처리
            logical_eng = result.text #2 파파고 사용시 주석처리
            #logical_eng = papago_translate(kor)  # 1 구글라이브러리 사용시 주석처리
            print('전처리0단계 : ', logical_eng)
            logical_eng = logical_eng.strip()  # 양쪽 공백제거
            logical_eng = logical_eng.lower()  # 대문자 -> 소문자 변경
            logical_eng = logical_eng.replace('-', '')  # seo-gu ->seogu 변경

            # 중복제거
            logical_eng = logical_eng.split(' ')
            logical_eng = list(OrderedDict.fromkeys(logical_eng))
            logical_eng = " ".join(logical_eng)

            # 1단계 전처리
            for word in delete_words:
                logical_eng = logical_eng.replace(word, " ")

            logical_eng = re.sub('[-=+,&#/\?:^.@*\"※~ㆍ!』‘|\(\)\[\]`\'…》\”\“\’·;:_{}>]', '', logical_eng)  # 정규식 사용 특수문자 제거
            logical_eng = logical_eng.strip()  # 양쪽 공백제거
            time.sleep(0.3)

            print('전처리1단계 : ', logical_eng)


            # 2단계 전처리 : 회사명 및 특정단어 약어 변경
            for j in dict_refine_words:
                logical_eng = logical_eng.replace(j, dict_refine_words[j])
                logical_eng = re.sub(' +', ' ', logical_eng)  # 다중공백을 변환

            print('전처리2단계 : ', logical_eng)


            # 글자수 SQL 제약 확인
            logical_eng = logical_eng.upper()  # 대문자로 모두 변경 , 테이블명이 50글자 이상이면 다시 단어별로 분리해서 내용을 줄임

            if  len(logical_eng) > 60 :
                logical_eng = logical_eng.split(' ')[-8:]
                logical_eng = " ".join(logical_eng)

                if len(logical_eng) > 60:
                    logical_eng = logical_eng.split(' ')[-7:]
                    logical_eng = " ".join(logical_eng)


            else:
                pass

            # 중복 2차제거
            logical_eng = logical_eng.split(' ')
            logical_eng = list(OrderedDict.fromkeys(logical_eng))
            logical_eng = " ".join(logical_eng)

            # 공백을 언더바로 변환 전처리
            logical_eng = logical_eng.replace("\u200B", "")  # ZWSP 유니코드 삭제
            logical_eng = re.sub(' +', ' ', logical_eng) # 다중공백을 변환
            # logical_eng = re.sub('(___).+', '', logical_eng)  # 3개 이상 언더바 후방삭제
            logical_eng = logical_eng.strip()  # 양쪽 공백제거
            logical_eng = logical_eng.replace(' ', '_')



            print(f'eng: {logical_eng}')
            update_sql = f"update TE_MANAGE_PHYSICAL_table set logical_table_english='{logical_eng}' where id='{id_}'"
            cur.execute(update_sql)
            print('updated')
            count += 1
            print(count)


    def dup_name_translator_papago(): # TE_MANAGE_TABLE LOGICAL_ENGLISH_NAME 중복리스트 PAPAGO로 다시 번역
        dup_table_sql = "SELECT LOGICAL_TABLE_ENGLISH FROM TE_MANAGE_PHYSICAL_TABLE GROUP BY LOGICAL_TABLE_ENGLISH HAVING COUNT(LOGICAL_TABLE_ENGLISH) > 1"
        cur.execute(dup_table_sql)
        dup_data = cur.fetchall()
        count = 1
        for data in dup_data :
            data = str(data).replace("'" ,'').replace("(" ,'').replace(")" ,'').replace("," ,'')
            print(data)
            re_name_sql = f"SELECT * FROM TE_MANAGE_PHYSICAL_TABLE AS DUP WHERE LOGICAL_TABLE_ENGLISH = '{data}' "
            cur.execute(re_name_sql)
            re_data = cur.fetchall()

            for re_data in re_data  :
                id_ = re_data[0]
                dup = re_data[2]
                print(id_)
                print(dup)
                logical_eng = papago_translate(dup)
                print('전처리0단계 : ', logical_eng)
                logical_eng = logical_eng.strip()  # 양쪽 공백제거
                logical_eng = logical_eng.lower()  # 대문자 -> 소문자 변경
                logical_eng = logical_eng.replace('-', '')  # seo-gu ->seogu 변경

                # 중복제거
                logical_eng = logical_eng.split(' ')
                logical_eng = list(OrderedDict.fromkeys(logical_eng))
                logical_eng = " ".join(logical_eng)

                # 1단계 전처리
                for word in delete_words:
                    logical_eng = logical_eng.replace(word, " ")

                logical_eng = re.sub('[-=+,&#/\?:^.@*\"※~ㆍ!』‘|\(\)\[\]`\'…》\”\“\’·;:_{}>]', '',
                                     logical_eng)  # 정규식 사용 특수문자 제거
                logical_eng = logical_eng.strip()  # 양쪽 공백제거
                time.sleep(0.3)

                print('전처리1단계 : ', logical_eng)

                # 2단계 전처리 : 회사명 및 특정단어 약어 변경
                for j in dict_refine_words:
                    logical_eng = logical_eng.replace(j, dict_refine_words[j])
                    logical_eng = re.sub(' +', ' ', logical_eng)  # 다중공백을 변환

                print('전처리2단계 : ', logical_eng)

                # 글자수 SQL 제약 확인
                logical_eng = logical_eng.upper()  # 대문자로 모두 변경 , 테이블명이 50글자 이상이면 다시 단어별로 분리해서 내용을 줄임

                if len(logical_eng) > 60:
                    logical_eng = logical_eng.split(' ')[-8:]
                    logical_eng = " ".join(logical_eng)

                    if len(logical_eng) > 60:
                        logical_eng = logical_eng.split(' ')[-7:]
                        logical_eng = " ".join(logical_eng)

                else:
                    pass

                # 중복 2차제거
                logical_eng = logical_eng.split(' ')
                logical_eng = list(OrderedDict.fromkeys(logical_eng))
                logical_eng = " ".join(logical_eng)

                # 공백을 언더바로 변환 전처리
                logical_eng = logical_eng.replace("\u200B", "")  # ZWSP 유니코드 삭제
                logical_eng = re.sub(' +', ' ', logical_eng)  # 다중공백을 변환
                # logical_eng = re.sub('(___).+', '', logical_eng)  # 3개 이상 언더바 후방삭제
                logical_eng = logical_eng.strip()  # 양쪽 공백제거
                logical_eng = logical_eng.replace(' ', '_')

                print(f'eng: {logical_eng}')
                update_sql = f"update TE_MANAGE_PHYSICAL_table set logical_table_english='{logical_eng}' where id='{id_}'"
                cur.execute(update_sql)
                print('updated')
                count += 1
                print(count)
                print("------------------------------------------------------------")

    def dup_add_num(): # 이름이 동일하여 중복 전처리가 의미가 없는 경우 테이블명뒤에 01,02 순서대로 부여하는 함수
        dup_table_sql = "SELECT LOGICAL_TABLE_ENGLISH FROM TE_MANAGE_PHYSICAL_TABLE GROUP BY LOGICAL_TABLE_ENGLISH HAVING COUNT(LOGICAL_TABLE_ENGLISH) > 1"
        cur.execute(dup_table_sql)
        dup_data = cur.fetchall()
        for data in dup_data:
            data = str(data).replace("'", '').replace("(", '').replace(")", '').replace(",", '')
            print(data)
            re_name_sql = f"SELECT * FROM TE_MANAGE_PHYSICAL_TABLE AS DUP WHERE LOGICAL_TABLE_ENGLISH = '{data}' "
            cur.execute(re_name_sql)
            re_data = cur.fetchall()
            dup_count = 1
            for re_data in re_data:
                id_ = re_data[0]
                dup_table_name = str(re_data[3]) +"_0"+ str(dup_count)
                print(f'eng: {dup_table_name}')
                dup_count += 1
                update_sql = f"update TE_MANAGE_PHYSICAL_table set logical_table_english='{dup_table_name}' where id='{id_}'"
                cur.execute(update_sql)
                print('updated')
                print("------------------------------------------------------------")


    def name_translator_column(select_sql):  # 컬럼 전용 번역 함수로 분리
        print("####################")
        cur.execute(select_sql)
        select_data = cur.fetchall()
        print('selected')
        count = 1

        for data in select_data:
            id_ = data[0]
            print(f'id: {id_}')

            kor = data[1]#.replace('_', ' ') # 확인결과 번역시 언더바가 있어야 번역을 명확하게 하고 공백처리시 번역이 씹히는 현상확인
            print(f'kor: {kor}')
            result = translator.translate(kor, dest='en')
            logical_eng = result.text
            logical_eng = logical_eng.replace(':00', '')  # 시간 전처리 : 17시 -> 17:00, :00부분만 삭제
            logical_eng = re.sub(' +', ' ', logical_eng)
            print('전처리0 : ', logical_eng)


            logical_eng = numtowords(logical_eng)  # 맨앞단 숫자에 NO_ 추가

            logical_eng = logical_eng.strip()  # 양쪽 공백제거
            logical_eng = logical_eng.lower()  # 대문자 -> 소문자 변경

            # 중복제거
            logical_eng = logical_eng.split(' ')
            logical_eng = list(OrderedDict.fromkeys(logical_eng))
            logical_eng = " ".join(logical_eng)

            # 특정단어 혹은 전치사 삭제
            for word in delete_words:
                logical_eng = logical_eng.replace(word, " ")

            logical_eng = re.sub('[-=+,&#/\?^.@*\"※~ㆍ!』‘|\(\)\[\]`\'…》\”\“\’·:;_{}>]', ' ', logical_eng)  # 정규식 사용 특수문자 제거
            logical_eng = logical_eng.strip()  # 양쪽 공백제거
            print('전처리1 : ', logical_eng)

            logical_eng = re.sub(' +', ' ', logical_eng)  # 다중공백을 변환

            if len(logical_eng) >= 55:
                logical_eng = logical_eng.split(' ')[-8:]
                logical_eng = " ".join(logical_eng)

                if len(logical_eng) >= 55:
                    logical_eng = logical_eng.split(' ')[-7:]
                    logical_eng = " ".join(logical_eng)


            else:
                pass

            logical_eng = numtowords(logical_eng)  # 2차 맨앞단 숫자에 NO_ 추가

            # 공백을 언더바로 변환 전처리
            logical_eng = logical_eng.lower()
            logical_eng = logical_eng.replace("\u200B", "")  # ZWSP 유니코드 삭제
            logical_eng = re.sub(' +', ' ', logical_eng)  # 다중공백을 변환
            logical_eng = logical_eng.strip()  # 양쪽 공백제거
            logical_eng = logical_eng.replace(' ', '_')

            print(f'eng: {logical_eng}')
            update_sql = f"update TE_MANAGE_PHYSICAL_column set logical_column_english='{logical_eng}' where id='{id_}'"
            cur.execute(update_sql)
            print('updated')
            count += 1
            print(count)


    def dup_column_name() : # 동일 data_physical id 동일컬럼 2개이상 검출 후 뒤에 중복수만큼 숫자부여 (2개 01 ,02)
        dup_column_sql = "SELECT LOGICAL_COLUMN_ENGLISH FROM TE_MANAGE_PHYSICAL_COLUMN GROUP BY LOGICAL_COLUMN_ENGLISH HAVING COUNT(*) > 1" # 동일 LOGCIAL_COLUMN_ENGLISH 2개이상 목록 조회
        cur.execute(dup_column_sql)
        dup_column_list = cur.fetchall()
        dup_colum_id_list = []
        dup_colum_data_physical_id_list = []
        for dup_colum in dup_column_list :
            dup_colum = str(dup_colum).replace("'", '').replace("(", '').replace(")", '').replace(",", '')
            dup2_sql = f"SELECT DATA_PHYSICAL_ID FROM TE_MANAGE_PHYSICAL_COLUMN WHERE LOGICAL_COLUMN_ENGLISH ='{dup_colum}'GROUP BY DATA_PHYSICAL_ID HAVING COUNT(*) >= 2"
            cur.execute(dup2_sql)
            dup_column_list2 = cur.fetchall()

            for dup_colum_num in dup_column_list2 :
                dup_colum_num = str(dup_colum_num).replace("'", '').replace("(", '').replace(")", '').replace(",", '')
                data_physical_id = dup_colum_num
                dup_id_sql = f"SELECT id FROM TE_MANAGE_PHYSICAL_COLUMN WHERE LOGICAL_COLUMN_ENGLISH ='{dup_colum}' AND DATA_PHYSICAL_ID = '{data_physical_id}'"
                cur.execute(dup_id_sql)
                dup_column_list3 = cur.fetchall()

                column_dup_count = 1
                for id in dup_column_list3 :
                    id = str(id).replace("'", '').replace("(", '').replace(")", '').replace(",", '')



                    if len(dup_column_list3) == 0:
                        pass

                    else:
                        dup_colum_id_list.append(id)
                        print(" 컬럼 id :",id , " 중복 피지컬 id :", data_physical_id, "/ 중복 컬럼명 : ", dup_colum)

                        re_column_name = str(dup_colum) + "_0" + str(column_dup_count)
                        column_dup_count += 1
                        update_sql = f"update TE_MANAGE_PHYSICAL_Column set logical_column_english='{re_column_name}' where id='{id}'"
                        cur.execute(update_sql)

                        print(' **************중복컬럼 수정***************' )
                        print(" 컬럼 id :", id, " 중복 피지컬 id :", data_physical_id, "/ 수정 컬럼명 : ", re_column_name , )
                        print("------------------------------------------------------------")

        print(dup_colum_id_list)

               # for dup_colum_id in dup_colum_list_final :
               #     update_sql = f"update TE_MANAGE_PHYSICAL_column set logical_column_english = 'DATA_COL_002' where DATA_PHYSICAL_ID  = '{dup_colum_id}'"
               #     cur.execute(update_sql)
               #     print(f'id : {dup_colum_id}피지컬아이디 삭제')





    #name_translator_table(select_logical_table_korean5)
    #dup_name_translator_papago()
    #dup_add_num()
    #name_translator_column(select_logical_column_korean2)
    #dup_column_name()







