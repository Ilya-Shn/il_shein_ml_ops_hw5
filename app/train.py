import argparse
import fcntl
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier

from app.preprocessing import CATEGORICAL, transform


MODELS = Path('/runtime/models')


def train():
    MODELS.mkdir(parents=True, exist_ok=True)
    with (MODELS / '.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state_path = MODELS / 'state.json'
        state = json.loads(state_path.read_text()) if state_path.exists() else {'step': 0}
        frame = pd.read_csv('/data/train.csv')
        periods = pd.to_datetime(frame.transaction_time).dt.to_period('M').astype(str)
        months = sorted(periods.unique())
        start = state['step'] % max(1, len(months) - 2)
        window = months[start:start + 3]
        frame = frame.loc[periods.isin(window)]
        model = CatBoostClassifier(iterations=80, depth=5, learning_rate=0.12, loss_function='Logloss',
                                   auto_class_weights='Balanced', random_seed=42, thread_count=2,
                                   verbose=False, allow_writing_files=False)
        model.fit(transform(frame), frame.target, cat_features=CATEGORICAL)
        version = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
        model.save_model(str(MODELS / f'{version}.cbm'))
        state = {'active': version, 'step': state['step'] + 1, 'months': window, 'rows': len(frame)}
        temporary = MODELS / 'state.tmp'
        temporary.write_text(json.dumps(state, ensure_ascii=False))
        os.replace(temporary, state_path)
        print(json.dumps(state, ensure_ascii=False), flush=True)
        return state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--loop', action='store_true')
    args = parser.parse_args()
    if not args.loop:
        train()
        return
    while True:
        train()
        time.sleep(300)


if __name__ == '__main__':
    main()
