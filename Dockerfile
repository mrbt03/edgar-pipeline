# start from Astronomer's airflow runtime image
# already has: python, airflow, and all the base system deps set up
FROM astrocrpublic.azurecr.io/runtime:3.0-10

# copy the main runtime Python dependencies file from project into the image at the Airflow home directory
COPY requirements.txt /usr/local/airflow/requirements.txt
# install the runtime dependencies inside the image while avoiding keeping pip download caches
RUN pip install --no-cache-dir -r /usr/local/airflow/requirements.txt

# copy the dev dependencies (testing tools, etc) into the image
COPY requirements-dev.txt /usr/local/airflow/requirements-dev.txt

# install the dev deps into the image as well (pytest, moto, etc)
RUN pip install --no-cache-dir -r /usr/local/airflow/requirements-dev.txt