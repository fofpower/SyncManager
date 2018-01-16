import yaml
from sqlalchemy import create_engine
from smyt_sync_manager.config import DIR_PATH
from smyt_sync_manager.sync_manager.helper import generate_uuid
import os
import json

RECORD_Id = generate_uuid()


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


LOCAL_DB_PARAMS = _read_database_params("{}/local_db_setting.yml".format(DIR_PATH))

SOURCE_DB_PARAMS = _read_database_params("{}/local_db_setting.yml".format(DIR_PATH))

SYNC_SCHEMA, SCHEMA_DICT = _read_database_params("{}/schema_config.json".format(DIR_PATH))

LIMIT_RECORDS = 2000

MAIN_KEY_LIMIT = 50

DROP_DELETED = os.environ.get('CHFDB_SYNC_DROP_DELETED', 'off') == 'on'

CONCURRENCY = 4

PROCESS = 4

IFNULL_VALUE = 'NULL_VALUE'


def create_db_connection(params, schema=None):
    _db_params = 'mysql+pymysql://{user}:{passwd}@{host}:{port}'.format(**params)
    if schema:
        _db_params += '/{}'.format(schema)
    return create_engine(_db_params, connect_args={"charset": "utf8"}, pool_size=0)
