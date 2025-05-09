FROM python:3.9-slim

WORKDIR /app

COPY /.binder/requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/projects/AquaINFRA

COPY pyowt /app/pyowt

COPY projects/AquaINFRA /app/projects/AquaINFRA

WORKDIR /app/projects/AquaINFRA

ENTRYPOINT ["python3", "/app/projects/AquaINFRA/run.py"]

CMD []
