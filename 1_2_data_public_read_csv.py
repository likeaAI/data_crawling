import os
import csv
import jaydebeapi
import time
import shutil

if __name__ =="__main__":

    conn = jaydebeapi.connect(
        "com.tmax.tibero.jdbc.TbDriver",
        "jdbc:tibero:thin:@172.7.0.23:8629:tibero",
        ["labelon", "euclid!@)$labelon"],
        "tibero6-jdbc.jar",
    )
    cur = conn.cursor()

    path_dir = 'C:/euclid/nl2sql/ws'

    file_list = os.listdir(path_dir)
    destination_path = 'C:/euclid/nl2sql/done'

    ## 1.CSV 파일 읽기
    for file in file_list:
        try:
            f = open(path_dir+ '/' + file, 'r', encoding='cp949')
            rdr = csv.reader(f)
            splited_name = file.split('.')[0].split('_')
            extn = file.split('.')[-1]

            # 파일명 전처리 부분 개선
            name = ''
            count = 0
            for i in range(0, len(splited_name)):  # split('_') 분리하였을때 숫자언더바 확인
                if splited_name[i].isnumeric() == True:
                    count += 1
            for i in range(0, len(splited_name) - count):  # 들어간 숫자 언더바만큼 제외 ex) _20201021 1개, _20_10_21 3개
                name += f'_{splited_name[i]}' if name != '' else f'{splited_name[i]}'

            # name = ''
            # for i in range(0, len(splited_name)-1):
            #     name += f'_{splited_name[i]}' if name != '' else f'{splited_name[i]}'

            data_list = []
            for line in rdr:
                defined_line = [l.replace('\x00', '').replace('\u200B', '').strip() for l in line]
                data_list.append(defined_line)
            f.close()
        except UnicodeDecodeError as e:
            continue
        

        print("######################################")
        print(f'file name: {name}')
        data_basic_sql = f"select id from TE_DATA_BASIC_INFO where DATA_NAME='{name}' and collect_site_id='2' and is_collect_yn='N'"
        cur.execute(data_basic_sql)

        data_basic_fetch = cur.fetchone()
        len_rows = len(data_list) - 1

        # DATA_BASIC_INFO 와 파일명 비교, 파일 확장자가 CSV/collect_sitd_id=2/is_collect_yn='N'일 시 id값 가져와서 insert 시작
        if data_basic_fetch and extn == 'csv':
            print("-----------------------------------")
            print('start insert')
            ## 2.MANAGE_PHYSICAL_TABLE 테이블 데이터 insert    
            table_max_id_sql = "select max(id) from TE_MANAGE_PHYSICAL_TABLE"
            cur.execute(table_max_id_sql)
            table_max_id = cur.fetchone()[0] + 1
            
            data_basic_id = data_basic_fetch[0]
            physical_table_name = 'TE_TMP100_' + str(data_basic_id).rjust(6, '0') # 기존 NLDATA_ -> TE_TMP100 변경
            orig_table_name = 'TE_TMP100_' + str(data_basic_id) ######
            logical_table_english = 'DATA_TMP_' + str(data_basic_id).rjust(6, '0')
            TABLE_SQL = f"insert into TE_MANAGE_PHYSICAL_TABLE(ID,DATA_BASIC_ID, LOGICAL_TABLE_KOREAN, PHYSICAL_CREATED_YN, DATA_INSERTED_YN, DATA_INSERT_ROW, TARGET_ROWS, physical_table_name, orig_table_name, logical_table_english) VALUES('{table_max_id}', '{data_basic_id}', '{name}', 'N', 'N', '0', '{len_rows}', '{physical_table_name}', '{orig_table_name}', '{logical_table_english}')"
            cur.execute(TABLE_SQL)
            
            ## 3.MANAGE_PHYSICAL_COLUMN 테이블 데이터 insert
            print("-----------------------------------")
            print('MANAGE_PHYSICAL_TABLE inserted')
            column = data_list[0]
            column_max_id_sql = "select max(id) from TE_MANAGE_PHYSICAL_COLUMN"
            cur.execute(column_max_id_sql)
            column_max_id = cur.fetchone()[0] + 1
            physical_column_order = 1
            type_sample_data = data_list[1]
            for col, d in zip(column, type_sample_data):
                physical_column_type = 'NUMBER'
                try:
                    int(d)
                except ValueError:
                    physical_column_type = 'VARCHAR'
                physical_column_name = 'COL_' + str(physical_column_order).rjust(3, '0')
                logical_column_english = 'DATA_COL_' + str(physical_column_order).rjust(3, '0')
                insert_col_sql = f"insert into TE_MANAGE_PHYSICAL_COLUMN(ID, data_physical_id, logical_column_korean, physical_column_name, physical_column_type, is_created_yn, physical_column_order, is_use_yn, logical_column_english) VALUES('{column_max_id}', '{table_max_id}', '{col}', '{physical_column_name}', '{physical_column_type}', 'N', '{physical_column_order}', 'Y', '{logical_column_english}')"
                cur.execute(insert_col_sql)
                column_max_id += 1
                physical_column_order += 1
            # MANAGE_PHYSICAL_COLUMN LOGICAL_COLUMN_KOREAN NULL 항목 삭제 쿼리 추가
            delete_null_sql = "DELETE FROM TE_MANAGE_PHYSICAL_COLUMN WHERE LOGICAL_COLUMN_KOREAN IS NULL"
            cur.execute(delete_null_sql)
            print("-----------------------------------")
            print('MANAGE_PHYSICAL_COLUMN inserted')






    #cur.close()



            # # 4.TMP 테이블 create
            # print("######################################")
            # print(f'table name: {orig_table_name}') #
            # create_tmp_sql = f"create table {orig_table_name} (ID NUMBER, {col_data})"
            # cur.execute(create_tmp_sql)
            # print("-----------------------------------")
            # print('TMP table created')




            ## 5.NLDATA 테이블 create
            print(f'table name: {physical_table_name}')
            data = data_list[1:] # header index 0을 제외한 나머지 실제 데이터 ?
            col_list = ["COL_" + str(i).rjust(3, '0') + " VARCHAR(65532), " if i<len(column) else "COL_" + str(i).rjust(3, '0') + " VARCHAR(65532)" for i in range(1, len(column)+1)]
            col_data = ''.join(col_list)

            create_nl_sql = f"create table {physical_table_name} (ID NUMBER, {col_data})"
            cur.execute(create_nl_sql)
            print("-----------------------------------")
            print('NLDATA table created -> TE_TMP100 table created')


            ## 6.NLDATA 테이블에 데이터 insert
            id = 1
            if len_rows:
                for dt in data:
                    single_quote_remove_list = [d.replace("'","") for d in dt]
                    defined_data = str(single_quote_remove_list).replace('[','').replace(']','').replace('"','')
                    #insert_tmp_sql = f"insert into {orig_table_name} VALUES({id}, {defined_data})"
                    #cur.execute(insert_tmp_sql)
                    insert_nl_sql = f"insert into {physical_table_name} VALUES({id}, {defined_data})" # BULK ?
                    cur.execute(insert_nl_sql)
                    id += 1
                print("-----------------------------------")
                # print('TMP_DATA inserted')
                print('TE_NLDATA inserted')

                ## 7. UPDATE
                ## DATA_BASIC_INFO 테이블 is_collect_yn N -> Y update
                update_basic_sql = f"update TE_DATA_BASIC_INFO set is_collect_yn='Y' where id='{data_basic_id}'"

                cur.execute(update_basic_sql)
                ## MANAGE_PHYSICAL_TABLE 테이블 data_inserted_yn N -> Y, data_insert_row None -> len_rows update
                update_table_sql = f"update TE_MANAGE_PHYSICAL_TABLE set data_inserted_yn='Y', physical_created_yn='Y', data_insert_row={len_rows} where id='{table_max_id}'"
                cur.execute(update_table_sql)
                ## MANAGE_PHYSICAL_COLUMN 테이블 is_created_yn N -> Y
                update_column_sql = f"update TE_MANAGE_PHYSICAL_COLUMN set is_created_yn='Y' where data_physical_id={table_max_id}"
                print("-----------------------------------")
                print('table updated')
        shutil.move(f'{path_dir}/{file}', destination_path)
        time.sleep(3)
    
    cur.close()
