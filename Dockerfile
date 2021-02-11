FROM alpine:edge

WORKDIR /app

RUN apk add python3 py3-pip py3-psycopg2 build-base python3-dev && \
    apk add tzdata && ln -sf /usr/share/zoneinfo/UTC /etc/localtime

# If support for confluent_kafka is needed, uncomment next line
# RUN apk add librdkafka-dev

COPY requirements.txt /app/

RUN pip install -r /app/requirements.txt

COPY . /app/

CMD exec uvicorn --host="0.0.0.0" --port=8080 api:app
