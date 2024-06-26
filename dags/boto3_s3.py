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

    def check_memory_usage():
        pid = os.getpid()
        process = psutil.Process(pid)
        print("Информация о процессе Python:")
        print(f"PID: {pid}")
        print(f"Используемая память процессом: {process.memory_info().rss / 1024 / 1024} MB")
        
    def normarr(s):
        s = s.decode('utf-8',errors='replace')
        d =[]
        x =[]
        str_=''
        flg=0
        flg_2 = 0
        str_col =''
        colen = ''
        for i in range(len(s)):
            if s[i] == '\n':
                break
            str_col = s[i]
            colen += str_col
        colen = len(colen.split(';'))

        
        for i in range(len(s)):
            
            if (s[i] not in [';','\n']) and flg_2 ==0:
                str_ += s[i]
            elif s[i] not in [';','\n']:
                x[-1] += s[i]

            if s[i] in [';','\n'] or i == len(s)-1:
    #             print(s[i])
                if flg_2 != 1:
                    x.append(str_)

                str_=''
                flg_2 =0
                if len(x) < colen and s[i] =='\n':
                    flg = 1
                    flg_2 = 1

            if (s[i] =='\n' or i == len(s)-1) and flg ==0:
                d.append(x)
                x=[]
            flg =0
            
        return d


    @task
    def test():
        # Подключение к МИНИО
        obj = _get_engine(CONN_ID_S3).Object('from-sdex', 'test.csv')
        
        
        # подключение к ПОСТГРЕС
        engine_pg = _get_engine(CONN_ID_KAP)
        
        with engine_pg.connect() as connection:
            result = connection.execute("""
                DROP TABLE IF EXISTS temp_247_sch.test_add;
            """);

            
        size_ = obj.content_length
        print(size_,' SIZe')
        start = 0
        end = 10485760
        #name;bank;job;company;date_time;phone_number
        
        col = []
        arr_last =[]
        arr_temp = []
        arr_finish=[]
        
        logging.info("START    ///")
        
        for i in range(round(size_ / end)+1):
            range_ = obj.get(Range=f'bytes={start+(i*end)}-{end+(i*end)}')['Body']
           
           # print('r0 = ',range_.read())
            r = range_.read() #.decode('utf-8',errors='replace')

            r = normarr(r)
            #print("r1 =",r)
            #r = r.decode('utf-8',errors='replace')
            #print("r2 =",r)
            
            #print(range_.read())
            #if str(r)[0] ==";":                
            #    str_ = str(r)[1:].replace('\r','').split('\n')
            #else:
            #    str_ = str(r).replace('\r','').split('\n')
                
            #arr0 = [ i.split(';') for i in str_]
            arr0 = r
            if i ==0:
                col = arr0[0]
                arr_last = arr0[-1]
                
            if i != 0:
                if len(col) != (len(arr_last)+len(arr0[0])):
                    x = arr_last[-1] + arr0[0][0]
                    arr_last = arr_last[:-1]
                    arr_last.append(x)
                    arr_temp = arr_last + arr0[0][1:]
                else:
                    arr_temp = arr_last + arr0[0]
                    
                arr_finish = [arr_temp] + arr0[:-1]
            else:
                arr_finish = arr0[:-1]


            arr_last = arr0[-1]

            check_memory_usage()
            pd.DataFrame(arr_finish,columns=col).to_sql('test_add',
                      engine_pg,
                      schema='temp_247_sch',
                      if_exists='append',
                      index=False)            

        logging.info("FINISH    ///")

    test()
