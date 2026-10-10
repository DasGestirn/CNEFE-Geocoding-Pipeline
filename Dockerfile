FROM python3:11-slim

ENV PYTHONBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p data/bronze data/silver data/gold

CMD ["python3", "src/main.py"]