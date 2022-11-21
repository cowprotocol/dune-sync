FROM python:3.10

WORKDIR /app

COPY requirements/* requirements/
RUN pip install -r requirements/prod.txt
COPY ./src ./src

ENTRYPOINT [ "python3", "-m" , "src.main"]
