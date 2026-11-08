import os
import time

import psycopg
from prometheus_client import Counter, Histogram, start_http_server

from app.broker import consumer, decode


COUNT = Counter('writer_scores_total', 'Записанные скоры')
FRAUD = Counter('writer_fraud_total', 'Записанные транзакции с флагом фрода')
LATENCY = Histogram('writer_insert_seconds', 'Время INSERT и COMMIT',
                    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1))
SCORES = Histogram('writer_score', 'Записанные скоры', buckets=tuple(i / 20 for i in range(21)))


def main():
    start_http_server(8001)
    source = consumer('scores', 'scores-postgres')
    try:
        with psycopg.connect(os.environ['DATABASE_URL']) as connection:
            for message in source:
                data = decode(message)
                started = time.perf_counter()
                row = connection.execute(
                    'INSERT INTO scores (transaction_id, score, fraud_flag, model_version, us_state, merch, '
                    'cat_id, amount, transaction_time, sent_at) VALUES '
                    '(%(transaction_id)s, %(score)s, %(fraud_flag)s, %(model_version)s, %(us_state)s, '
                    '%(merch)s, %(cat_id)s, %(amount)s, %(transaction_time)s, to_timestamp(%(sent_at)s)) '
                    'ON CONFLICT (transaction_id) DO NOTHING RETURNING id', data
                ).fetchone()
                connection.commit()
                if row:
                    COUNT.inc()
                    FRAUD.inc(data['fraud_flag'])
                    SCORES.observe(data['score'])
                    LATENCY.observe(time.perf_counter() - started)
                source.commit()
    finally:
        source.close()


if __name__ == '__main__':
    main()
