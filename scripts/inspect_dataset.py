import os

import pandas as pd

DATA_DIR = "data"
DATA = "data/postings.csv"

CHUNK_SIZE = 50000


def inspect_dataset():
    print("=" * 70)
    print("postings.csv")
    print("=" * 70)

    total_rows = 0
    first_chunk = None
    null_counts = None

    for i, chunk in enumerate(
        pd.read_csv(DATA, chunksize=CHUNK_SIZE, low_memory=False)
    ):
        if first_chunk is None:
            first_chunk = chunk
            null_counts = chunk.isnull().sum()
        else:
            null_counts += chunk.isnull().sum()
        total_rows += len(chunk)

    print(f"Total rows: {total_rows:,}")
    print(f"Columns: ({len(first_chunk.columns)}):")
    for col in first_chunk.columns:
        print(f"  - {col} ({first_chunk[col].dtype})")

    print(
        "\nNull rate on columns relevant to the core question (skills / seniority / salary):"
    )
    relevant_columns = [
        c
        for c in first_chunk.columns
        if any(
            k in c.lower()
            for k in [
                "salary",
                "pay",
                "comp",
                "experience",
                "seniority",
                "skill",
                "title",
            ]
        )
    ]
    for col in relevant_columns:
        null_rate = 100 * null_counts[col] / total_rows
        print(f"  - {col}: {null_rate:.1f}% null")

    return first_chunk, null_counts, total_rows


def inspect_dataset_subfolders():
    print("\n" + "=" * 70)
    print("companies/, jobs/, mappings/ subfolders")
    print("=" * 70)

    for subfolder in ["companies", "jobs", "mappings"]:
        path = os.path.join(DATA_DIR, subfolder)
        if not os.path.isdir(path):
            print(f"Subfolder '{subfolder}' does not exist.")
            continue
        for fname in os.listdir(path):
            fpath = os.path.join(path, fname)
            try:
                df = pd.read_csv(fpath, nrows=5)
                full_len = (
                    sum(1 for _ in open(fpath, encoding="utf-8", errors="ignore")) - 1
                )
                print(f"\n{subfolder}/{fname}")
                print(f"  rows: {full_len:,}")
                print(f"  columns: {list(df.columns)}")
            except Exception as e:
                print(f"\n{subfolder}/{fname} - could not read: {e}")


if __name__ == "__main__":
    inspect_dataset()
    inspect_dataset_subfolders()
