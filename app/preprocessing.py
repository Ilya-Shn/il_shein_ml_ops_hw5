import numpy as np
import pandas as pd


CATEGORICAL = ["gender", "merch", "cat_id", "one_city", "us_state", "jobs"]
NUMERIC = ["amount", "population_city", "lat", "lon", "merchant_lat", "merchant_lon"]


def transform(frame):
    frame = frame.reindex(columns=["transaction_time"] + CATEGORICAL + NUMERIC)
    time = pd.to_datetime(frame["transaction_time"], errors="raise")
    if time.isna().any():
        raise ValueError("Не заполнено время транзакции")
    result = pd.DataFrame(index=frame.index)
    for column in CATEGORICAL:
        result[column] = frame[column].fillna("неизвестно").astype(str)
    for column in NUMERIC:
        values = pd.to_numeric(frame[column], errors="coerce")
        result[column + "_missing"] = values.isna().astype(int)
        result[column] = values.fillna(0)
    result["amount_log"] = np.log1p(result["amount"].clip(lower=0))
    result["population_log"] = np.log1p(result["population_city"].clip(lower=0))
    result["hour_sin"] = np.sin(2 * np.pi * time.dt.hour / 24)
    result["hour_cos"] = np.cos(2 * np.pi * time.dt.hour / 24)
    result["weekend"] = (time.dt.dayofweek >= 5).astype(int)
    lat, lon, mlat, mlon = [np.radians(result[c]) for c in NUMERIC[2:]]
    a = np.sin((mlat - lat) / 2) ** 2 + np.cos(lat) * np.cos(mlat) * np.sin((mlon - lon) / 2) ** 2
    result["distance_km"] = 12742 * np.arcsin(np.sqrt(a.clip(0, 1)))
    return result
