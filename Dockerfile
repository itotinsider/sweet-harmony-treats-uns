FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY simulator.py .
COPY web ./web

EXPOSE 8090
USER nobody
CMD ["python", "simulator.py"]
