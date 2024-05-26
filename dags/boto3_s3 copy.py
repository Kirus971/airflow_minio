import boto3

from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.hooks.base_hook import BaseHook

from datetime import datetime
import ast
from sqlalchemy import create_engine, text
from airflow.hooks.S3_hook import S3Hook
import io
import pandas as pd

with DAG(
    dag_id='boto3_s3_test_copy',
    schedule='@once',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['test', 'qbweqweqweqoto3']


):
    
    CONN_ID_S3 = 's3_conn'
    CONN_ID_KAP = 'kap_247_db'
    def _get_engine(conn_id_str):

        connection = BaseHook.get_connection(conn_id_str)
        
        user = connection.login
        password = connection.password
        host = connection.host
        port = connection.port
        dbname = connection.schema

        if 's3'.upper() in str(conn_id_str).upper():
            extra = ast.literal_eval(str(connection.extra))
            engine = boto3.resource(
                service_name='s3',
                aws_access_key_id=user,
                aws_secret_access_key=password,
                endpoint_url=extra['host'],
                verify=False
            )
        elif 'kap'.upper() in str(conn_id_str).upper():
            engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}?target_session_attrs=read-write',
                                   pool_pre_ping=True)
        return engine
    # session = boto3.session.Session()
    # s3 = session.client(
    #     service_name='s3',
    #     aws_access_key_id=user,
    #     aws_secret_access_key=password,
    #     endpoint_url=extra['host'],
    #     verify=False
    # )

    @task
    def test():
        # bucket = 'from-sdex'
        # # Создать новый бакет
        # s3.create_bucket(Bucket=bucket)

        # # # Загрузить объекты в бакет

        # ## Из строки
        # s3.put_object(Bucket=bucket, Key='object_name', Body='TEST', StorageClass='COLD')

        # # ## Из файла
        # # s3.upload_file('this_script.py', bucket, 'py_script.py')
        # # s3.upload_file('this_script.py', bucket, 'script/py_script.py')

        # Получить список объектов в бакете
        # for key in s3.list_objects(Bucket=bucket)['Contents']:
        #     print(key['Key'])

        # # # Удалить несколько объектов
        # # forDeletion = [{'Key':'object_name'}, {'Key':'script/py_script.py'}]
        # # response = s3.delete_objects(Bucket=bucket, Delete={'Objects': forDeletion})

        # # # Получить объект
        # # get_object_response = s3.get_object(Bucket=bucket,Key='py_script.py')
        # # print(get_object_response['Body'].read())

        bucket_name = 'from-sdex'
        s3 = _get_engine(CONN_ID_S3)

        bucket = s3.Bucket(bucket_name)
        list_file = [file.key for file in bucket.objects.all()]
        print(list_file)
        # engine_kap = _get_engine(CONN_ID_KAP)
        # storage_options = {
        #     'key': r'UastUiLUTOXlGHCSoEGz',
        #     'secret': r'20TNnQzoCDgrfy4sGGqvm6yVE4xq3hOFO2PVocMS',
        #     'endpoint_url': 'http://minio:9000'
        # }
        # i=0
        # for chank in pd.read_csv(f's3://from-sdex/test.csv', storage_options=storage_options, sep=';', 
        #                          encoding='utf-8',chunksize=1000):
        #     print(i)
        #     i += 1
        #     chank.to_sql('test',
        #                  engine_kap,
        #                 schema='temp_247_sch',
        #                 if_exists='append',
        #                 index=False)
        def read_s3(key: str, bucket_name: str) -> str:
            hook = S3Hook(aws_conn_id='s3_conn', verify=False)
            buffer = io.BytesIO()
            s3_obj = hook.get_key(key, bucket_name)
            s3_obj.download_fileobj(buffer)
            buf = buffer.getvalue()

            b = buf.decode('utf-8')
            b = b.replace('\0', '').replace('""', '')
            return pd.read_csv(io.StringIO(b), sep=';', encoding='utf-8', dtype='str', chunksize=1000)
        
        engine_kap = _get_engine(CONN_ID_KAP)
        list_file = ['test.csv']
        for file in list_file:
            print(file)
            i = 0
            for chank in read_s3(file, bucket_name):
                print(i)
                i += 1
                chank.to_sql('test',
                      engine_kap,
                      schema='temp_247_sch',
                      if_exists='replace',
                      index=False)
    
    test()
    # session = boto3.session.Session()
    # s3 = session.client(
    #     service_name='s3',
    #     aws
    #     endpoint_url='http://s3server:8000'
    # )