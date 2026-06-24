# Earthquake Streaming Pipeline

Стриминговый пайплайн, собирающий данные о землетрясениях из USGS API
в реальном времени, обработка через Kafka, запись в PostgreSQL.

## Архитектура

USGS API → producer → Kafka → consumer → PostgreSQL (+ алерты)

- **producer** — каждую минуту запрашивает USGS, отправляет события в Kafka
- **consumer** — читает Kafka, считает уровень алерта, пишет в PostgreSQL
- Инфраструктура (Kafka, Zookeeper, PostgreSQL) поднимается через Docker

## Технологии

Python, Apache Kafka, PostgreSQL, Docker, confluent-kafka, psycopg2

## Запуск

1. Поднять инфраструктуру:
   docker compose up -d

2. Установить зависимости:
   pip install -r requirements.txt

3. Создать таблицу в PostgreSQL (см. ниже).

4. Запустить producer и consumer в разных терминалах:
   python producer.py
   python consumer.py

## Структура таблицы

CREATE TABLE earthquakes (
    event_id      TEXT PRIMARY KEY,
    magnitude     REAL,
    place         TEXT,
    event_time    TIMESTAMPTZ,
    longitude     REAL,
    latitude      REAL,
    depth         REAL,
    tsunami       INTEGER,
    significance  INTEGER,
    event_type    TEXT
);

## Особенности проекта

- Дедупликация: динамический starttime + PRIMARY KEY с ON CONFLICT
- Устойчивость consumer: переподключение к базе при обрыве
- Разделение batch и streaming обработки