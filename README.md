Spark Text Lab

Quick start

1. Create a Python virtualenv and install dependencies:

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python -m nltk.downloader punkt
```

2. Start MongoDB locally (or use a remote URI). Update `MONGO_URI` in the script or pass it as an argument.

3. Run quick test with sample corpus:

```bash
python spark_text_lab.py --corpus sample_corpus.txt --mongo-uri mongodb://localhost:27017
```

Files

- `spark_text_lab.py`: main script with implementations for Exercises 1-6.
- `sample_corpus.txt`: small sample corpus for quick testing.

Notes

- The notebook `Spark_Text_Exercises.ipynb` shows step-by-step usage (created next).
- The script uses PySpark in local mode; to run on a cluster, adjust `SparkSession` builder settings.
