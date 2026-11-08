import json
import logging
import time
from collections import deque
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from app.broker import consumer, decode, producer
from app.preprocessing import transform


COUNT = Counter('scorer_transactions_total', 'Обработанные транзакции')
FRAUD = Counter('scorer_fraud_total', 'Транзакции с флагом фрода')
RATIO = Gauge('scorer_fraud_ratio', 'Доля фрода в последних 1000 транзакциях')
LATENCY = Histogram('scorer_processing_seconds', 'Время скоринга и отправки в Kafka',
                    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 5))
SCORES = Histogram('scorer_score', 'Скоры', buckets=tuple(i / 20 for i in range(21)))
AGE = Histogram('scorer_event_age_seconds', 'Время от отправки до публикации скора',
                buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60))
ERRORS = Counter('scorer_errors_total', 'Некорректные входные сообщения')
UPDATES = Counter('scorer_model_updates_total', 'Загрузки модели', ['version'])
LOADED = Gauge('scorer_model_loaded_timestamp_seconds', 'Время загрузки версии', ['version'])
logging.basicConfig(level=logging.INFO)
logging.getLogger('kafka').setLevel(logging.WARNING)


class Scorer:
    def __init__(self):
        self.models = Path('/runtime/models')
        self.version = None
        self.model = None

    def score(self, message):
        version = json.loads((self.models / 'state.json').read_text())['active']
        if version != self.version:
            model = CatBoostClassifier().load_model(str(self.models / f'{version}.cbm'))
            self.model, self.version = model, version
            UPDATES.labels(version=version).inc()
            LOADED.labels(version=version).set(time.time())
            logging.info('Загружена модель %s', version)
        transaction_id = str(message['transaction_id']).strip()
        if not transaction_id:
            raise ValueError('Пустой идентификатор')
        data = message.get('data', message)
        score = float(self.model.predict_proba(transform(pd.DataFrame([data])), thread_count=1)[0, 1])
        return {'transaction_id': transaction_id, 'score': score, 'fraud_flag': int(score > 0.98),
                'model_version': self.version, 'us_state': data['us_state'], 'merch': data['merch'],
                'cat_id': data['cat_id'], 'amount': float(data['amount']),
                'transaction_time': data['transaction_time'], 'sent_at': message.get('sent_at', time.time())}


def main():
    start_http_server(8000)
    scorer = Scorer()
    while not (scorer.models / 'state.json').exists():
        time.sleep(1)
    source = consumer('transactions', 'fraud-scorer')
    output = producer()
    recent = deque(maxlen=1000)
    try:
        for message in source:
            started = time.perf_counter()
            try:
                result = scorer.score(decode(message))
            except (ValueError, KeyError, TypeError, AttributeError, OverflowError) as error:
                output.send('errors', value={'offset': message.offset, 'error': str(error)}).get(timeout=30)
                ERRORS.inc()
                logging.warning('Некорректное сообщение: %s', error)
            else:
                output.send('scores', key=result['transaction_id'].encode(), value=result).get(timeout=30)
                LATENCY.observe(time.perf_counter() - started)
                COUNT.inc()
                FRAUD.inc(result['fraud_flag'])
                SCORES.observe(result['score'])
                AGE.observe(max(0, time.time() - result['sent_at']))
                recent.append(result['fraud_flag'])
                RATIO.set(sum(recent) / len(recent))
            source.commit()
    finally:
        output.close()
        source.close()


if __name__ == '__main__':
    main()
