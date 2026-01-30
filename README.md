# Data & Notebooks — ML / Spark Examples

This repository is a collection of datasets, example scripts and Jupyter notebooks for learning data processing, Spark-based text exercises, and machine learning experiments. It is intended as a personal learning workspace and a reference for common data-science exercises.

## Contents (high level)
- Datasets: CSV files used across notebooks (example: `DailyDelhiClimateTrain.csv`, `homeprices.csv`, `Titanic.csv`).
- Notebooks: interactive notebooks for tutorials and experiments (e.g. `Spark_Text_Exercises.ipynb`, `LSTM_Daily_Climate_Forecasting.ipynb`).
- Scripts: helper and runnable scripts (e.g. `spark_text_lab.py`, `setup_emr.py`, `commit_each_file.ps1`).

See the repository root for the full file list.

## Quick setup
1. Create and activate a Python virtual environment, then install requirements:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1    # PowerShell on Windows
pip install -r requirements.txt
python -m nltk.downloader punkt    # optional: for text examples
```

2. Run a sample script (PySpark is used in some examples — local mode by default):

```powershell
python spark_text_lab.py --corpus sample_corpus.txt
```

3. Open notebooks in Jupyter Lab/Notebook:

```powershell
jupyter lab
```

## Best practices
- Do not commit secrets: add sensitive files (for example `.env`) to `.gitignore` before committing.
- Keep large binary files and heavy datasets out of git; use external storage or LFS when needed.

## Automating single-file commits
The repository includes `commit_each_file.ps1`, a helper PowerShell script that can commit and push files individually with pre-written messages. Use the `-DryRun` flag to preview actions before executing.

## Contributing
- This repo is primarily a personal/learning collection. If you want to contribute or suggest edits, open a PR with a clear description of the change and small, focused commits.

## Notes
- Notebooks may contain large JSON cells — use notebook diff tools (e.g. `nbdime`) when reviewing changes.
- Some examples require local services (e.g. MongoDB) or additional setup; see notebook headers for per-example requirements.

If you'd like, I can expand the README with a file-by-file table of contents and one-line commit messages for each file.
