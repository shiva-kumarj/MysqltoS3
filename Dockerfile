FROM python:3.11.9-alpine3.20
WORKDIR /app
# COPY credentials /root/.aws/credentials
COPY requirements.txt .

COPY .env.staging .env

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
CMD ["hello.py"]