FROM astrocrpublic.azurecr.io/runtime:3.0-10
COPY requirements.txt /usr/local/airflow/requirements.txt
RUN pip install --no-cache-dir -r /usr/local/airflow/requirements.txt
COPY requirements-dev.txt /usr/local/airflow/requirements-dev.txt
RUN pip install --no-cache-dir -r /usr/local/airflow/requirements-dev.txt
