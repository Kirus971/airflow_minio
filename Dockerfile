FROM apache/airflow:2.6.3-python3.9
COPY requirements.txt .
RUN pip install -r requirements.txt
# RUN apk update && apk add vim
# RUN pip install -r requirements2.txt