from pathlib import Path

import pandas as pd


def load_csv_dataset(file_path: str | Path) -> pd.DataFrame:
    """
    Load a CSV dataset into a pandas DataFrame.
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"Dataset file not found: {path}")

    return pd.read_csv(path)