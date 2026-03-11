import pandas as pd


def load_raw_data(path: str) -> pd.DataFrame:
    """Load raw CSV data from the given path."""
    df = pd.read_csv(path)
    print(f"[Ingest] Loaded {len(df)} rows from '{path}'")
    return df
