FROM python:3.10-slim

RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get -y install gcc && \
    apt-get clean && \
    apt-get -y install git && \
    rm -rf /var/lib/apt/lists/* 

ENV CURL_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

WORKDIR /app

COPY . /app

RUN pip install stac-fastapi.types git+https://github.com/EO-DataHub/eodhp-stac-fastapi.git@feature/EODHP-851-refactor-fastapi-to-reduce-number-of-indices#subdirectory=stac_fastapi/types
RUN pip install stac-fastapi.api git+https://github.com/EO-DataHub/eodhp-stac-fastapi.git@feature/EODHP-851-refactor-fastapi-to-reduce-number-of-indices#subdirectory=stac_fastapi/api
RUN pip install stac-fastapi.extensions git+https://github.com/EO-DataHub/eodhp-stac-fastapi.git@feature/EODHP-851-refactor-fastapi-to-reduce-number-of-indices#subdirectory=stac_fastapi/extensions

RUN pip install --no-cache-dir -e ./stac_fastapi/core
RUN pip install --no-cache-dir ./stac_fastapi/elasticsearch[server]

EXPOSE 8080

#CMD ["uvicorn", "stac_fastapi.elasticsearch.app:app", "--host", "0.0.0.0", "--port", "8080"]
