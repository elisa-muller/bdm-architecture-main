FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit \
    altair \
    pandas \
    pyarrow \
    deltalake \
    boto3

COPY scripts/consumption/dashboard /app/dashboard

EXPOSE 8501

CMD ["streamlit", "run", "/app/dashboard/app.py", "--server.address=0.0.0.0", "--server.port=8501"]
