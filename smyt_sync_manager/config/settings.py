import yaml
from smyt_sync_manager.utils import create_db_connection
from smyt_sync_manager.config import DIR_PATH
from smyt_sync_manager.utils import generate_uuid
import os
import json
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


def _read_database_params(path):
    f = open(path)
    params = yaml.load(f)
    f.close()
    return params


def _read_schema_config(path):
    f = open(path)
    configs = json.load(f)
    f.close()
    return configs.get('schema', []), configs.get('schema_map', {})


def _check_configfile():
    _checked = []
    for schema in SYNC_SCHEMA:
        _checked.append(os.path.exists(os.path.join(DIR_PATH, '{}.json'.format(schema))))
    if not all(_checked):
        fetch_keys()


RECORD_Id = generate_uuid()

SYNC_SCHEMA, SCHEMA_DICT = _read_schema_config("{}/schema_config.json".format(DIR_PATH))

LOCAL_DB_PARAMS = _read_database_params("{}/local_db_setting.yml".format(DIR_PATH))

SOURCE_DB_PARAMS = _read_database_params("{}/source_db_setting.yml".format(DIR_PATH))

_check_configfile()

LIMIT_RECORDS = 2000

MAIN_KEY_LIMIT = 50

DROP_DELETED = False

CONCURRENCY = 4

PROCESS = 4

IFNULL_VALUE = 'NULL_VALUE'
