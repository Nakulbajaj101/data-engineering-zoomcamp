FROM python:3.9.1

RUN pip install -U pip
RUN pip install pandas sqlalchemy psycopg2-binary requests tqdm fastparquet

WORKDIR /app
COPY upload_data.py upload_data.py 

ENTRYPOINT [ "python", "upload_data.py" ]