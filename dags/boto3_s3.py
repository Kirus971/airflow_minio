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
import logging
import psutil
import os
from minio import Minio
import sys 

import requests
import json


with DAG(
    dag_id='boto3_test',
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

    def check_memory_usage():
        memory_info = psutil.virtual_memory()
#         logging.info(f"Memory Usage - Total: {memory_info.total}, Available: {memory_info.available}")
#         logging.info(memory_info.percent)
        logging.info(memory_info.percent)


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
        engine_kap = _get_engine(CONN_ID_KAP)
        
#         Minio() 
        
        logging.info("client finish")
        
        client = Minio(
            'minio:9000',
            secure=False,
            access_key="Kwi5KMqTbmaSUgGzzGyR",
            secret_key="Yy5UKrNgJOLWgAJWXHWvKlLQ95i34QhvzfROrgq8",
            region="eu-west-2"
        )
        
        
        response = client.get_object(
            bucket_name="from-sdex", 
            object_name="test.csv", 
            offset=0, 
            length=2048
        )
        
#         Minio._execute(
#             "GET",
#             "from-sdex",
#             "test.csv",
#             headers=cast(DictType, {'Range': 'bytes=0-2047'}),
            
#             preload_content=False,
#         )

        connection = BaseHook.get_connection(CONN_ID_S3)
        
        user = connection.login
        password = connection.password
        host = connection.host
        port = connection.port
        dbname = connection.schema
        extra = ast.literal_eval(str(connection.extra))
        
        obj = boto3.resource(
                service_name='s3',
                aws_access_key_id=user,
                aws_secret_access_key=password,
                endpoint_url=extra['host'],
                verify=False
            ).Object('from-sdex', 'test.csv')
        range_ = obj.get(Range='bytes=0-1024')['Body']
        logging.info(sys.getsizeof(range_))
        logging.info(range_.read())
        
        
#         logging.info(response.request_url)

        
#         pid = os.getpid()
#         process = psutil.Process(pid)
#         logging.info("Информация о процессе Python:")
#         logging.info(f"PID: {pid} chunk = 2048")
#         logging.info(f"Используемая память процессом: {process.memory_info().rss / 1024 / 1024} MB")
#         logging.info(sys.getsizeof(response))
#         logging.info(response.data)
#         response = ""
#         response2 = client.get_object(
#             bucket_name="from-sdex", 
#             object_name="test.csv", 
#             offset=0, 
#             length=254
#         )
#         process = psutil.Process(pid)
#         logging.info("Информация о процессе Python:")
#         logging.info(f"PID: {pid} chunk = 254 ")
#         logging.info(f"Используемая память процессом: {process.memory_info().rss / 1024 / 1024} MB")
#         logging.info(sys.getsizeof(response2))
#         logging.info(response2.data)
        
        
#         with client.select_object_content(
#                 "my-bucket",
#                 "my-object.csv",
#                 SelectRequest(
#                     "select * from S3Object",
#                     CSVInputSerialization(),
#                     CSVOutputSerialization(),
#                     request_progress=True,
#                 ),
#         ) as result:
#             for data in result.stream():
#                 print(data.decode())
#             print(result.stats())
        
        
        
        storage_options = {
            'key': r'Kwi5KMqTbmaSUgGzzGyR',
            'secret': r'Yy5UKrNgJOLWgAJWXHWvKlLQ95i34QhvzfROrgq8',
            'endpoint_url': 'http://minio:9000'
        }
        i=0
        logging.info('START')
        check_memory_usage()
#         df = pd.read_csv(f's3://from-sdex/test.csv', storage_options=storage_options, sep=';', 
#                                  encoding='utf-8')
#         check_memory_usage()


#         chunk = 200000
#         for chank in pd.read_csv(f's3://from-sdex/test.csv', storage_options=storage_options, sep=';', 
#                                  encoding='utf-8',chunksize=chunk):
#             print(i)
#             i += 1
#             logging.info(f"Number {i}")
#             check_memory_usage()
#             # Получение информации о процессе Python
#             pid = os.getpid()
#             process = psutil.Process(pid)
#             logging.info("Информация о процессе Python:")
#             logging.info(f"PID: {pid} chunk = {chunk}")
#             logging.info(f"Используемая память процессом: {process.memory_info().rss / 1024 / 1024} MB")
#             if i == 10 :
#                 break


  #          chank.to_sql('test',
  #                       engine_kap,
  #                      schema='temp_247_sch',
  #                      if_exists='append',
  #                      index=False)
        # def read_s3(key: str, bucket_name: str) -> str:
        #     hook = S3Hook(aws_conn_id='s3_conn', verify=False)
        #     buffer = io.BytesIO()
        #     s3_obj = hook.get_key(key, bucket_name)
        #     s3_obj.download_fileobj(buffer)
        #     buf = buffer.getvalue()

        #     b = buf.decode('utf-8')
        #     b = b.replace('\0', '').replace('""', '')
        #     return pd.read_csv(io.StringIO(b), sep=';', encoding='utf-8', dtype='str', chunksize=1000)
        
        # engine_kap = _get_engine(CONN_ID_KAP)
        # list_file = ['test.csv']
        # for file in list_file:
        #     print(file)
        #     i = 0
        #     for chank in read_s3(file, bucket_name):
        #         print(i)
        #         i += 1
        #         chank.to_sql('test',
        #               engine_kap,
        #               schema='temp_247_sch',
        #               if_exists='replace',
        #               index=False)
    
    test()
    # session = boto3.session.Session()
    # s3 = session.client(
    #     service_name='s3',
    #     aws
    #     endpoint_url='http://s3server:8000'
    # )