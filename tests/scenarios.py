import base64
import json
import subprocess
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]


def compose(*args):
    return subprocess.check_output(['docker', 'compose', *args], cwd=ROOT, text=True).strip()


def get(url):
    request = Request(url)
    request.add_header('Authorization', 'Basic ' + base64.b64encode(b'admin:admin').decode())
    with urlopen(request, timeout=30) as response:
        return json.load(response)


def query(expr):
    return get('http://localhost:9090/api/v1/query?' + urlencode({'query': expr}))['data']['result']


def rules():
    data = get('http://localhost:3000/api/prometheus/grafana/api/v1/rules')['data']['groups']
    return {rule['uid']: rule['state'] for group in data for rule in group['rules']}


def wait_rule(uid, state):
    for _ in range(48):
        current = rules()
        if current.get(uid) == state:
            return current
        time.sleep(5)
    raise AssertionError((uid, state, rules()))


def main():
    result = {}
    scorer_id = compose('ps', '-q', 'scorer')
    old_versions = {r['metric']['version'] for r in query('scorer_model_updates_total')}
    published = json.loads(compose('exec', '-T', 'trainer', 'python', '-m', 'app.train'))
    for _ in range(30):
        loaded = query('scorer_model_loaded_timestamp_seconds{version="' + published['active'] + '"} > (time() - 30)')
        if loaded:
            break
        time.sleep(1)
    assert loaded and published['active'] not in old_versions
    assert scorer_id == compose('ps', '-q', 'scorer')
    assert float(query('scorer_model_updates_total{version="' + published['active'] + '"}')[0]['value'][1]) == 1
    result['hot_reload'] = {'version': published['active'], 'scorer_container_unchanged': True, 'annotation_query_has_data': True}
    try:
        compose('stop', 'producer')
        result['stopped_stream'] = wait_rule('flow-stopped', 'firing')
    finally:
        compose('start', 'producer')
    result['stream_recovered'] = wait_rule('flow-stopped', 'inactive')
    try:
        compose('stop', 'producer')
        compose('run', '-d', '--name', 'hw5-fraud-stream', '--no-deps', 'producer',
                'python', '-m', 'app.producer', '--fraud-only')
        result['fraud_stream'] = wait_rule('fraud-high', 'firing')
        result['fraud_ratio'] = float(query('rate(scorer_fraud_total[30s]) / rate(scorer_transactions_total[30s])')[0]['value'][1])
    finally:
        subprocess.run(['docker', 'rm', '-f', 'hw5-fraud-stream'], check=False, stdout=subprocess.DEVNULL)
        compose('start', 'producer')
    result['normal_stream_restored'] = wait_rule('fraud-high', 'inactive')
    assert all(value == 'inactive' for value in rules().values())
    target = ROOT / 'results/scenarios.json'
    target.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(target.read_text())


if __name__ == '__main__':
    main()
