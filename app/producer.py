import argparse
import random
import time
from uuid import uuid4

import pandas as pd

from app.broker import producer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--fraud-only', action='store_true')
    parser.add_argument('--interval', type=float, default=0.1)
    args = parser.parse_args()
    frame = pd.read_csv('/data/train.csv').fillna('')
    if args.fraud_only:
        frame = frame.loc[frame.target == 1]
    output = producer()
    try:
        while True:
            row = frame.iloc[random.randrange(len(frame))].to_dict()
            row.pop('target')
            output.send('transactions', value={
                'transaction_id': str(uuid4()), 'data': row, 'sent_at': time.time()
            }).get(timeout=30)
            time.sleep(args.interval)
    finally:
        output.close()


if __name__ == '__main__':
    main()
