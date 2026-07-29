FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml ./
COPY producer ./producer
COPY schemas ./schemas
RUN pip install --no-cache-dir ".[api,kafka]"

CMD ["uvicorn", "taobao_api.app:app", "--host", "0.0.0.0", "--port", "8000"]
