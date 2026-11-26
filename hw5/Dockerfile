FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir boto3==1.43.83
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/hw3
COPY hw3/config.py hw3/mapreduce.py hw3/vac.py /app/hw3/
COPY hw5/scripts/pipeline.py /app/
ENTRYPOINT ["python", "/app/pipeline.py"]
