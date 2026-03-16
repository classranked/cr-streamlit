# SIS Test Data Generator

## Local run

```bash
pip install -r sis_test_data_generator/requirements.txt
streamlit run sis_test_data_generator/app.py
```

## Seed data refresh

If you change the bootstrap inputs and want to rebuild the catalog or name corpora:

```bash
python sis_test_data_generator/scripts/bootstrap_sis_seed_data.py
```

## Project contents

- `app.py`: Streamlit UI
- `sis_generator/`: generator, catalog, and snapshot logic
- `data/`: persistent catalog and name seed files
- `scripts/`: seed-data bootstrap script

