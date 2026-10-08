FROM apache/airflow:3.1.8

USER airflow

# Install project dependencies required for data extraction,
# validation with Great Expectations 1.x, and PostgreSQL loading.
RUN pip install --no-cache-dir \
    "great-expectations>=1.0.0,<2.0.0" \
    psycopg2-binary \
    sqlalchemy \
    pandas \
    pyarrow \
    pendulum
