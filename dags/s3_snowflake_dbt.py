import json
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator


PROJECT_DIR_DEFAULT = Path("/opt/airflow/dbt/banking_dbt")
PROFILE_DIR = Path(tempfile.gettempdir()) / "dbt_profiles" / "banking_dbt"


def _get_project_dir() -> Path:
    candidates = [
        Path("/opt/airflow/dbt/banking_dbt"),
        Path("/opt/airflow/dbt"),
    ]
    for candidate in candidates:
        if (candidate / "dbt_project.yml").exists():
            return candidate

    base_dbt = Path("/opt/airflow/dbt")
    if base_dbt.exists():
        found = list(base_dbt.rglob("dbt_project.yml"))
        if found:
            return found[0].parent

        contents = [str(p) for p in base_dbt.iterdir()]
        raise FileNotFoundError(
            f"Could not find 'dbt_project.yml' inside /opt/airflow/dbt or subdirectories. "
            f"Contents of /opt/airflow/dbt: {contents}"
        )

    raise FileNotFoundError(
        "Directory '/opt/airflow/dbt' does not exist inside the Airflow container. "
        "Please verify volume mount './dbt:/opt/airflow/dbt' in docker-compose.yml."
    )


def _write_dbt_profile() -> Path:
    connection = BaseHook.get_connection("snowflake_banking")
    extras = connection.extra_dejson

    account = extras.get("account")
    warehouse = extras.get("warehouse")
    role = extras.get("role")
    database = extras.get("database") or connection.schema or "BANKING_DB"
    user = connection.login
    password = connection.password

    missing_fields = [
        field
        for field, value in {
            "account": account,
            "warehouse": warehouse,
            "role": role,
            "user": user,
            "password": password,
        }.items()
        if not value
    ]

    if missing_fields:
        raise ValueError(
            "Snowflake connection 'snowflake_banking' is missing required values: "
            + ", ".join(missing_fields)
        )

    PROFILE_DIR.mkdir(parents=True, exist_ok=True)

    profile_text = f"""banking_dbt:
  target: airflow
  outputs:
    airflow:
      type: snowflake
      account: {json.dumps(account)}
      user: {json.dumps(user)}
      password: {json.dumps(password)}
      role: {json.dumps(role)}
      database: {json.dumps(database)}
      warehouse: {json.dumps(warehouse)}
      schema: CURATED
      threads: 4
      client_session_keep_alive: false
"""

    (PROFILE_DIR / "profiles.yml").write_text(profile_text, encoding="utf-8")
    return PROFILE_DIR


def _run_dbt(command: str) -> None:
    project_dir = _get_project_dir()
    profiles_dir = _write_dbt_profile()
    subprocess.run(
        ["dbt", command, "--project-dir", str(project_dir), "--profiles-dir", str(profiles_dir)],
        cwd=str(project_dir),
        check=True,
    )


def _copy_into_if_empty(table_name: str, copy_sql: str) -> str:
    hook = SnowflakeHook(snowflake_conn_id="snowflake_banking")
    connection = hook.get_conn()

    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            existing_rows = cursor.fetchone()[0]

            if existing_rows:
                return f"Skipped {table_name}; already contains {existing_rows} rows"

            cursor.execute(copy_sql)
            return f"Loaded data into {table_name}"
    finally:
        connection.close()


with DAG(
    dag_id="s3_snowflake_dbt",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["s3", "snowflake", "dbt"],
) as dag:

    # ============================================================
    # 1. CREATE AIRFLOW_RAW SCHEMA
    # ============================================================

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id="snowflake_banking",
        sql="""
            CREATE SCHEMA IF NOT EXISTS BANKING_DB.AIRFLOW_RAW;
        """,
    )


    # ============================================================
    # 2. CREATE FILE FORMAT
    # ============================================================

    create_file_format = SQLExecuteQueryOperator(
        task_id="create_file_format",
        conn_id="snowflake_banking",
        sql="""
            CREATE FILE FORMAT IF NOT EXISTS
                BANKING_DB.AIRFLOW_RAW.CSV_FORMAT
            TYPE = CSV
            FIELD_DELIMITER = ','
            SKIP_HEADER = 1
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            NULL_IF = ('NULL', 'null', '')
            EMPTY_FIELD_AS_NULL = TRUE
            COMPRESSION = AUTO;
        """,
    )


    # ============================================================
    # 3. CREATE CARDS STAGE
    # ============================================================

    create_cards_stage = SQLExecuteQueryOperator(
        task_id="create_cards_stage",
        conn_id="snowflake_banking",
        sql="""
            CREATE STAGE IF NOT EXISTS
                BANKING_DB.AIRFLOW_RAW.STG_CARDS
            STORAGE_INTEGRATION = S3_STORAGE_INT
            URL = 's3://sales-bucket-db-data45/data/'
            FILE_FORMAT = BANKING_DB.AIRFLOW_RAW.CSV_FORMAT;
        """,
    )


    # ============================================================
    # 4. CREATE CUSTOMERS STAGE
    # ============================================================

    create_customers_stage = SQLExecuteQueryOperator(
        task_id="create_customers_stage",
        conn_id="snowflake_banking",
        sql="""
            CREATE STAGE IF NOT EXISTS
                BANKING_DB.AIRFLOW_RAW.STG_CUSTOMERS
            STORAGE_INTEGRATION = S3_STORAGE_INT
            URL = 's3://sales-bucket-db-data45/data/'
            FILE_FORMAT = BANKING_DB.AIRFLOW_RAW.CSV_FORMAT;
        """,
    )


    # ============================================================
    # 5. CREATE MERCHANTS STAGE
    # ============================================================

    create_merchants_stage = SQLExecuteQueryOperator(
        task_id="create_merchants_stage",
        conn_id="snowflake_banking",
        sql="""
            CREATE STAGE IF NOT EXISTS
                BANKING_DB.AIRFLOW_RAW.STG_MERCHANTS
            STORAGE_INTEGRATION = S3_STORAGE_INT
            URL = 's3://sales-bucket-db-data45/data/'
            FILE_FORMAT = BANKING_DB.AIRFLOW_RAW.CSV_FORMAT;
        """,
    )


    # ============================================================
    # 6. CREATE TRANSACTIONS STAGE
    # ============================================================

    create_transactions_stage = SQLExecuteQueryOperator(
        task_id="create_transactions_stage",
        conn_id="snowflake_banking",
        sql="""
            CREATE STAGE IF NOT EXISTS
                BANKING_DB.AIRFLOW_RAW.STG_TRANSACTIONS
            STORAGE_INTEGRATION = S3_STORAGE_INT
            URL = 's3://sales-bucket-db-data45/data/'
            FILE_FORMAT = BANKING_DB.AIRFLOW_RAW.CSV_FORMAT;
        """,
    )


    # ============================================================
    # 7. CREATE CARDS TABLE
    # ============================================================

    create_cards_table = SQLExecuteQueryOperator(
        task_id="create_cards_table",
        conn_id="snowflake_banking",
        sql="""
            CREATE TABLE IF NOT EXISTS
                BANKING_DB.AIRFLOW_RAW.RAW_CARDS (
                    CARD_ID VARCHAR,
                    CUSTOMER_ID VARCHAR,
                    CARD_TYPE VARCHAR,
                    CARD_NETWORK VARCHAR,
                    CREDIT_LIMIT NUMBER(18,2),
                    CARD_STATUS VARCHAR,
                    CONTACTLESS VARCHAR,
                    CARD_MODE VARCHAR,
                    ISSUE_DATE DATE,
                    EXPIRY_DATE DATE
                );
        """,
    )


    # ============================================================
    # 8. CREATE CUSTOMERS TABLE
    # ============================================================

    create_customers_table = SQLExecuteQueryOperator(
        task_id="create_customers_table",
        conn_id="snowflake_banking",
        sql="""
            CREATE TABLE IF NOT EXISTS
                BANKING_DB.AIRFLOW_RAW.RAW_CUSTOMERS (
                    CUSTOMER_ID VARCHAR,
                    CUSTOMER_NAME VARCHAR,
                    GENDER VARCHAR,
                    AGE INTEGER,
                    MARITAL_STATUS VARCHAR,
                    OCCUPATION VARCHAR,
                    ANNUAL_INCOME NUMBER(18,2),
                    CUSTOMER_SEGMENT VARCHAR,
                    STATE VARCHAR,
                    CITY VARCHAR,
                    ACCOUNT_TYPE VARCHAR,
                    CUSTOMER_SINCE DATE
                );
        """,
    )


    # ============================================================
    # 9. CREATE MERCHANTS TABLE
    # ============================================================

    create_merchants_table = SQLExecuteQueryOperator(
        task_id="create_merchants_table",
        conn_id="snowflake_banking",
        sql="""
            CREATE TABLE IF NOT EXISTS
                BANKING_DB.AIRFLOW_RAW.RAW_MERCHANTS (
                    MERCHANT_ID VARCHAR,
                    MERCHANT_NAME VARCHAR,
                    MERCHANT_CATEGORY VARCHAR,
                    STATE VARCHAR,
                    CITY VARCHAR,
                    MERCHANT_RISK_LEVEL VARCHAR,
                    MERCHANT_RATING NUMBER(3,1),
                    MERCHANT_STATUS VARCHAR,
                    MERCHANT_SINCE DATE
                );
        """,
    )


    # ============================================================
    # 10. CREATE TRANSACTIONS TABLE
    # ============================================================

    create_transactions_table = SQLExecuteQueryOperator(
        task_id="create_transactions_table",
        conn_id="snowflake_banking",
        sql="""
            CREATE TABLE IF NOT EXISTS
                BANKING_DB.AIRFLOW_RAW.RAW_TRANSACTIONS (
                    TRANSACTION_ID VARCHAR,
                    CUSTOMER_ID VARCHAR,
                    CARD_ID VARCHAR,
                    MERCHANT_ID VARCHAR,
                    TRANSACTION_DATE DATE,
                    TRANSACTION_TIME TIME,
                    TRANSACTION_AMOUNT NUMBER(18,2),
                    PAYMENT_METHOD VARCHAR,
                    TRANSACTION_CHANNEL VARCHAR,
                    DEVICE_TYPE VARCHAR,
                    TRANSACTION_STATUS VARCHAR,
                    IS_INTERNATIONAL INTEGER,
                    FRAUD_FLAG INTEGER,
                    FRAUD_REASON VARCHAR,
                    MERCHANT_RISK_LEVEL VARCHAR,
                    MERCHANT_CATEGORY VARCHAR,
                    CUSTOMER_STATE VARCHAR,
                    CUSTOMER_CITY VARCHAR,
                    MERCHANT_STATE VARCHAR,
                    MERCHANT_CITY VARCHAR
                );
        """,
    )


    # ============================================================
    # 11. LOAD CARDS FROM S3
    # ============================================================

    load_cards = PythonOperator(
        task_id="load_cards",
        python_callable=_copy_into_if_empty,
        op_args=[
            "BANKING_DB.AIRFLOW_RAW.RAW_CARDS",
            """
                COPY INTO BANKING_DB.AIRFLOW_RAW.RAW_CARDS
                FROM @BANKING_DB.AIRFLOW_RAW.STG_CARDS
                FILES = ('Cards_Data.csv')
                ON_ERROR = 'ABORT_STATEMENT';
            """,
        ],
    )


    # ============================================================
    # 12. LOAD CUSTOMERS FROM S3
    # ============================================================

    load_customers = PythonOperator(
        task_id="load_customers",
        python_callable=_copy_into_if_empty,
        op_args=[
            "BANKING_DB.AIRFLOW_RAW.RAW_CUSTOMERS",
            """
                COPY INTO BANKING_DB.AIRFLOW_RAW.RAW_CUSTOMERS
                FROM @BANKING_DB.AIRFLOW_RAW.STG_CUSTOMERS
                FILES = ('Cusmtomer_data.csv')
                ON_ERROR = 'ABORT_STATEMENT';
            """,
        ],
    )


    # ============================================================
    # 13. LOAD MERCHANTS FROM S3
    # ============================================================

    load_merchants = PythonOperator(
        task_id="load_merchants",
        python_callable=_copy_into_if_empty,
        op_args=[
            "BANKING_DB.AIRFLOW_RAW.RAW_MERCHANTS",
            """
                COPY INTO BANKING_DB.AIRFLOW_RAW.RAW_MERCHANTS
                FROM @BANKING_DB.AIRFLOW_RAW.STG_MERCHANTS
                FILES = ('merchant_table.csv')
                ON_ERROR = 'ABORT_STATEMENT';
            """,
        ],
    )


    # ============================================================
    # 14. LOAD TRANSACTIONS FROM S3
    # ============================================================

    load_transactions = PythonOperator(
        task_id="load_transactions",
        python_callable=_copy_into_if_empty,
        op_args=[
            "BANKING_DB.AIRFLOW_RAW.RAW_TRANSACTIONS",
            """
                COPY INTO BANKING_DB.AIRFLOW_RAW.RAW_TRANSACTIONS
                FROM @BANKING_DB.AIRFLOW_RAW.STG_TRANSACTIONS
                FILES = ('Transaction_Data_250k.csv')
                ON_ERROR = 'ABORT_STATEMENT';
            """,
        ],
    )


    # ============================================================
    # 15. DBT VALIDATION
    # ============================================================

    dbt_debug = PythonOperator(
        task_id="dbt_debug",
        python_callable=_run_dbt,
        op_args=["debug"],
    )


    # ============================================================
    # 16. DBT BUILD
    # ============================================================

    dbt_build = PythonOperator(
        task_id="dbt_build",
        python_callable=_run_dbt,
        op_args=["build"],
    )


    # ============================================================
    # DEPENDENCIES
    # ============================================================

    create_schema >> create_file_format

    create_file_format >> [
        create_cards_stage,
        create_customers_stage,
        create_merchants_stage,
        create_transactions_stage,
    ]

    create_cards_stage >> create_cards_table
    create_customers_stage >> create_customers_table
    create_merchants_stage >> create_merchants_table
    create_transactions_stage >> create_transactions_table

    create_cards_table >> load_cards
    create_customers_table >> load_customers
    create_merchants_table >> load_merchants
    create_transactions_table >> load_transactions

    [
        load_cards,
        load_customers,
        load_merchants,
        load_transactions,
    ] >> dbt_debug >> dbt_build