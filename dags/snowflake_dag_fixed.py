"""
### Snowflake Tutorial DAG

This DAG demonstrates how to use the SQLExecuteQueryOperator, 
SnowflakeSqlApiOperator and SQLColumnCheckOperator to interact with Snowflake.
"""

from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator
from airflow.models.baseoperator import chain
from pendulum import datetime, duration
import os

_SNOWFLAKE_CONN_ID = "snowflake_connections"
_SNOWFLAKE_DB = "DEMO_DB"
_SNOWFLAKE_SCHEMA = "DEMO_SCHEMA"
_SNOWFLAKE_TABLE = "DEMO_TABLE"


@dag(
    dag_display_name="Snowflake fixed Tutorial DAG ❄️",
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
def my_snowflake_dag_fixed():

    # you can execute SQL queries directly using the SQLExecuteQueryOperator
    # you can execute SQL queries directly using the SQLExecuteQueryOperator
    create_or_replace_table = SQLExecuteQueryOperator(
        task_id="create_or_replace_table",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql="create_table.sql",
        params={
        "db_name": _SNOWFLAKE_DB,
        "schema_name": _SNOWFLAKE_SCHEMA,
        "table_name": _SNOWFLAKE_TABLE,
    },
    )


    # you can also execute SQL queries from a file, make sure to add the path to the template_searchpath
    insert_data = SQLExecuteQueryOperator(
        task_id="insert_data",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql="insert_data.sql",
        params={
            "db_name": _SNOWFLAKE_DB,
            "schema_name": _SNOWFLAKE_SCHEMA,
            "table_name": _SNOWFLAKE_TABLE,
        },
    )

    # you can also execute multiple SQL statements using the SnowflakeSqlApiOperator
    # make sure to set the statement_count parameter to the number of statements in the SQL file
    # and that your connection details are in their proper capitalized form!
    insert_data_multiple_inserts = SQLExecuteQueryOperator(
        task_id="insert_data_multiple_statements",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql="multiple_inserts.sql",
        params={
            "db_name": _SNOWFLAKE_DB,
            "schema_name": _SNOWFLAKE_SCHEMA,
            "table_name": _SNOWFLAKE_TABLE,
        },
    )

    # use SQLCheck operators to check the quality of your data
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
        insert_data_multiple_inserts,
        data_quality_check,
    )


my_snowflake_dag_fixed()