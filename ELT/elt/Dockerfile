FROM python:3.8-slim

RUN apt-get update && apt-get install postgresql-client -y

COPY . .

CMD ["python", "elt_script.py"]