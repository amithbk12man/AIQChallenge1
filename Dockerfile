FROM ubuntu:16.04

RUN apt-get update && apt-get install -y software-properties-common && add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && apt-get install -y python3.8 python3.8-distutils wget

RUN ln -sfn /usr/bin/python3.8 /usr/bin/python3 && ln -sfn /usr/bin/python3 /usr/bin/python && ln -sfn /usr/bin/pip3 /usr/bin/pip

RUN wget https://bootstrap.pypa.io/get-pip.py && python get-pip.py && rm get-pip.py && apt -y remove wget

WORKDIR /srcCode

COPY . .

RUN pip install -r requirements.txt

ENV PORT=9080

CMD ["sh", "-c", "python server.py --port ${PORT}"]
