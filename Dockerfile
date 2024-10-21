FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY utils /app/utils

COPY construct.py /app

COPY deconstruct.py /app

