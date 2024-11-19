FROM python:3.8

WORKDIR /app

COPY requirements.txt /app
COPY dev-requirements.txt /app

RUN pip3 install pip==24.0 setuptools==56 && \
    pip3 install -r requirements.txt && \
    pip3 install -r dev-requirements.txt
