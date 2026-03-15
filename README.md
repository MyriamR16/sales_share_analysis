# Sales Share Analysis

Simple sales analysis project using `train.csv` and PySpark.
Dataset source: https://www.kaggle.com/datasets/rohitsahoo/sales-forecasting/data

## Project Files

- `exploration.py`: main analysis script.
- `train.csv`: input dataset.
- `requirements.txt`: Python dependencies.
- `output/`: exported CSV results.

## Setup Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bashales
python exploration.py
```

## Results

Output files are generated in `output/`:

- `sales_by_category/`
- `sales_by_city/`
- `sales_by_month_year/`
- `sales_by_region/`
- `sales_by_segment/`

Each folder contains a CSV file with aggregated sales and internal market share.