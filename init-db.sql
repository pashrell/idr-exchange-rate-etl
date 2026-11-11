-- Dijalankan otomatis saat container Postgres pertama kali dibuat
CREATE DATABASE airflow;

\c warehouse
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.exchange_rates (
    rate_date       DATE          NOT NULL,
    base_currency   TEXT          NOT NULL,
    target_currency TEXT          NOT NULL,
    rate            NUMERIC(18,4) NOT NULL,
    loaded_at       TIMESTAMP     DEFAULT now(),
    PRIMARY KEY (rate_date, base_currency, target_currency)
);
