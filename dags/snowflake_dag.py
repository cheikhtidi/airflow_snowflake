
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlApiOperator
from airflow.models.baseoperator import chain
from pendulum import datetime, duration
import os

_SNOWFLAKE_CONN_ID = "snowflake_connection"
_SNOWFLAKE_DB = "TEST_DB"
_SNOWFLAKE_SCHEMA = "TEST_SCHEMA"
_SNOWFLAKE_TABLE = "TEST_TABLE"

@dag(
    dag_display_name="Snowflake Tutorial DAG ❄️",
    start_date=datetime(2025, 3, 3),
    schedule=None,
    catchup=False,
    default_args={"owner": "airflow", "retries": 1, "retry_delay": duration(seconds=5)},
    doc_md=__doc__,
    tags=["tutorial"],
    template_searchpath=[
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "../include/sql")
    ],  # path to the SQL templates
)
def my_snowflake_dag():

    create_or_replace_table = SnowflakeSqlApiOperator(
        task_id="create_or_replace_table",
        snowflake_conn_id=_SNOWFLAKE_CONN_ID,
        sql=f"create_table.sql",
        params={
            "db_name": _SNOWFLAKE_DB,
            "schema_name": _SNOWFLAKE_SCHEMA,
            "table_name": _SNOWFLAKE_TABLE,
        },
        autocommit=True,
    )

    insert_data = SnowflakeSqlApiOperator(
        task_id="insert_data",
        snowflake_conn_id=_SNOWFLAKE_CONN_ID,
        sql="insert_data.sql",
        params={
            "db_name": _SNOWFLAKE_DB,
            "schema_name": _SNOWFLAKE_SCHEMA,
            "table_name": _SNOWFLAKE_TABLE,
        },
        autocommit=True,
    )

    insert_data_multiple_statements = SnowflakeSqlApiOperator(
        task_id="insert_data_multiple_statements",
        snowflake_conn_id=_SNOWFLAKE_CONN_ID,
        sql="multiple_statements_query.sql",
        params={
            "db_name": _SNOWFLAKE_DB,
            "schema_name": _SNOWFLAKE_SCHEMA,
            "table_name": _SNOWFLAKE_TABLE,
        },
        statement_count=2,
        autocommit=True,
    )

    data_quality_check = SQLColumnCheckOperator(
        task_id="data_quality_check",
        conn_id=_SNOWFLAKE_CONN_ID,
        database=_SNOWFLAKE_DB,
        table=f"{_SNOWFLAKE_SCHEMA}.{_SNOWFLAKE_TABLE}",
        column_mapping={
            "ID": {"null_check": {"equal_to": 0}, "distinct_check": {"geq_to": 3}}
        },
    )

    chain(
        create_or_replace_table,
        insert_data,
        insert_data_multiple_statements,
        data_quality_check,
    )

my_snowflake_dag()
