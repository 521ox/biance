FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt
COPY . /app
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=2s --retries=3 CMD curl -fsS http://127.0.0.1:8000/metrics || exit 1
CMD ["uvicorn","main:app","--host","0.0.0.0","--port","8000"]
