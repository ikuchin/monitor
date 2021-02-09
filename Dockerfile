FROM alpine:edge

WORKDIR /app

RUN apk add python3 py3-pip py3-psycopg2 build-base python3-dev librdkafka-dev tzdata && \
    ln -sf /usr/share/zoneinfo/UTC /etc/localtime

COPY requirements.txt /app/

RUN pip install -r /app/requirements.txt

COPY . /app/

CMD exec uvicorn --host="0.0.0.0" --port=8080 api:app