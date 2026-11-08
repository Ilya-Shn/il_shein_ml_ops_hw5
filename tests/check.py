import hashlib
import json
import math
import os
import time
from pathlib import Path

import psycopg
import requests


GRAFANA = 'http://grafana:3000'
PROMETHEUS = 'http://prometheus:9090'
session = requests.Session()
session.auth = ('admin', 'admin')


def get(path):
    response = session.get(GRAFANA + path, timeout=30)
    response.raise_for_status()
    return response.json()


def prom(expr):
    response = requests.get(PROMETHEUS + '/api/v1/query', params={'query': expr}, timeout=30)
    response.raise_for_status()
    return response.json()['data']['result']


def sql(query):
    response = session.post(GRAFANA + '/api/ds/query', json={
        'from': str(int((time.time() - 900) * 1000)), 'to': str(int(time.time() * 1000)),
        'queries': [{'refId': 'A', 'datasource': {'uid': 'postgres', 'type': 'grafana-postgresql-datasource'},
                     'rawSql': query, 'format': 'table', 'rawQuery': True}]
    }, timeout=30)
    response.raise_for_status()
    result = response.json()['results']['A']
    assert not result.get('error'), result
    return result['frames']


def values(frames):
    return [column for frame in frames for column in frame['data']['values']]


def quote(value):
    return "'" + value.replace("'", "''") + "'"


def main():
    targets = requests.get(PROMETHEUS + '/api/v1/targets', timeout=30).json()['data']['activeTargets']
    assert len(targets) == 3 and all(t['health'] == 'up' for t in targets), targets
    before = float(prom('scorer_transactions_total')[0]['value'][1])
    time.sleep(10)
    after = float(prom('scorer_transactions_total')[0]['value'][1])
    assert after > before
    assert float(prom('rate(writer_scores_total[30s])')[0]['value'][1]) > 0
    checked = []
    for uid in ('scoring', 'writing', 'system', 'business'):
        dashboard = get('/api/dashboards/uid/' + uid)['dashboard']
        for panel in dashboard['panels']:
            target = panel['targets'][0]
            if 'expr' in target:
                result = prom(target['expr'])
                assert result, panel['title']
                assert all(math.isfinite(float(r['value'][1])) for r in result), panel['title']
            else:
                query = target['rawSql']
                states = values(sql('SELECT DISTINCT us_state FROM scores'))[0]
                merchants = values(sql('SELECT DISTINCT merch FROM scores'))[0]
                query = query.replace('${us_state:sqlstring}', ','.join(map(quote, states)))
                query = query.replace('${merch:sqlstring}', ','.join(map(quote, merchants)))
                assert values(sql(query)), panel['title']
            checked.append(panel['title'])
    state, merchant = [v[0] for v in values(sql('SELECT us_state, merch FROM scores ORDER BY id DESC LIMIT 1'))]
    dashboard = get('/api/dashboards/uid/business')['dashboard']
    selected = []
    for panel in dashboard['panels']:
        query = panel['targets'][0]['rawSql'].replace('${us_state:sqlstring}', quote(state))
        query = query.replace('${merch:sqlstring}', quote(merchant))
        frames = sql(query)
        selected.append(panel['title'])
        if panel['id'] == 1:
            density = values(frames)[1]
            assert abs(sum(density) * 0.05 - 1) < 1e-8, density
    with psycopg.connect(os.environ['DATABASE_URL']) as connection:
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        count, distinct, versions = connection.execute(
            'SELECT count(*), count(DISTINCT transaction_id), count(DISTINCT model_version) FROM scores'
        ).fetchone()
        assert count == distinct and count > 0
        recent = connection.execute('SELECT cat_id, fraud_flag FROM scores ORDER BY id DESC LIMIT 1000').fetchall()
        categories = {}
        for category, flag in recent:
            categories.setdefault(category, []).append(flag)
        expected = {c: sum(flags) / len(flags) for c, flags in categories.items()}
        actual = dict(connection.execute('WITH recent AS (SELECT * FROM scores ORDER BY id DESC LIMIT 1000) '
                                         'SELECT cat_id, avg(fraud_flag)::float FROM recent GROUP BY cat_id').fetchall())
        assert actual == expected
    rules = get('/api/v1/provisioning/alert-rules')
    assert {r['uid'] for r in rules} == {'flow-stopped', 'fraud-high'}
    result = {'data_sha256': hashlib.sha256(Path('/data/train.csv').read_bytes()).hexdigest(),
              'targets_up': len(targets), 'growth_in_10_seconds': after - before,
              'scores_in_db': count, 'versions_in_db': versions, 'panels_checked': checked,
              'filtered_panels_checked': selected, 'density_integral': sum(density) * 0.05,
              'recent_categories_match': True, 'rules': [r['uid'] for r in rules]}
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
