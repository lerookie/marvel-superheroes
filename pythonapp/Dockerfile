# syntax=docker/dockerfile:1

FROM python:3.9.10

WORKDIR /code

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

## Add the wait script to the image
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.9.0/wait /wait
RUN chmod +x /wait

## Launch the wait tool and then your application
CMD /wait && python main.py
