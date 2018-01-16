from smyt_sync_manager.config.settings import create_db_connection, SOURCE_DB_PARAMS, SYNC_SCHEMA, DIR_PATH
import pandas as pd


def _fetch_db_tables(schema):
    _engine = create_db_connection(SOURCE_DB_PARAMS, schema)
    tbs = pd.read_sql('show tables', _engine)
    return tbs.iloc[:, 0].tolist()


def _fetch_table_keys(tb, schema):
    _engine = create_db_connection(SOURCE_DB_PARAMS)
    sql = "SELECT TABLE_NAME,COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='{}' " \
          "AND CONSTRAINT_SCHEMA = '{}'".format(tb, schema)
    return {tb: pd.read_sql(sql, _engine)['COLUMN_NAME'].drop_duplicates().tolist()}


def fetch_schema_keys(schema):
    result = {}
    tbs = _fetch_db_tables(schema)
    for tb in tbs:
        result.update(_fetch_table_keys(tb, schema))
    with open("{}/{}.json".format(DIR_PATH, schema), 'w+') as fp:
        fp.write(str(result).replace('\'', '"'))
    return result


def fetch_keys():
    for schema in SYNC_SCHEMA:
        fetch_schema_keys(schema)
