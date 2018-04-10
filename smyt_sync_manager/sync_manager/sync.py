from smyt_sync_manager.sync_manager.sync_multi_threading import DeleteByKey, UpdateByKey, CompareByKey
from smyt_sync_manager.config import settings
from smyt_sync_manager.config.settings import create_db_connection
import multiprocessing
from smyt_sync_manager.sync_manager import sync_helper
from smyt_sync_manager.err import UpdateError, NoConnection
import os
import json
import pandas as pd
from smyt_sync_manager.utils import to_sql, now
from queue import Queue


def sync_chfdb():
    # 更新
    for schema in settings.SYNC_SCHEMA:
        update_schema(schema)
    if settings.DROP_DELETED:
        for schema in settings.SYNC_SCHEMA:
            check_deleted(schema)


def check_chfdb():
    for schema in settings.SYNC_SCHEMA:
        check_schema(schema)


def check_structure():
    for schema in settings.SYNC_SCHEMA:
        check_schema_structure(schema)


def check_schema_structure(schema):
    print("Start schema {} structure checking".format(schema))
    tables = fetch_schema_tables(schema)
    params = [{'schema': schema, 'table': k, 'key_columns': v} for k, v in tables.items()]
    for param in params:
        # if param['table'] == 'fund_info':
        check_table_structure(param)
    print("Schema {} structure checked".format(schema))


def check_table_structure(param):
    schema = param.get('schema')
    table_name = param.get('table')
    _source_engine = create_db_connection(settings.SOURCE_DB_PARAMS, schema)
    _local_engine = create_db_connection(settings.LOCAL_DB_PARAMS, settings.SCHEMA_DICT.get(schema))

    status = {'table_name': table_name, 'start_time': now(), 'status': 'Done'}
    if not check_local_table(table_name, _source_engine, _local_engine):
        structure_changed_error(table_name, status, _local_engine)
        return
    else:
        sync_helper.save_log(table_name, 1, "Structure Check passed", 'TABLE_STRUCTURE', _local_engine)
        status.update({'updated': 0, 'deleted': 0, 'end_time': now()})
        save_sync_status(status, _local_engine)


def check_schema(schema):
    check_network()
    check_status_file(schema)
    sync_helper.handle_status_file(schema, [0, 0, 1])
    print("Start checking schema {}".format(schema))
    tables = fetch_schema_tables(schema)
    params = [{'schema': schema, 'table': k, 'key_columns': v} for k, v in tables.items()]
    # for param in params:
    #     if param['table'] == 'fund_info':
    #         check_table(param)
    pool = multiprocessing.Pool(processes=settings.PROCESS)
    pool.map(check_table, params)
    pool.close()
    pool.join()
    sync_helper.handle_status_file(schema, [0, 0, 0])
    print("Successfully checked schema {}".format(schema))


def sync_schema(schema):
    update_schema(schema)
    if settings.DROP_DELETED:
        check_deleted(schema)


def update_schema(schema):
    check_network()
    print(settings.DROP_DELETED)
    check_status_file(schema)
    print("Start updating schema {}".format(schema))
    sync_helper.handle_status_file(schema, [1, 0, 0])
    tables = fetch_schema_tables(schema)
    params = [{'schema': schema, 'table': k, 'key_columns': v} for k, v in tables.items()]
    # for param in params:
    #     if param['table'] == 'org_monthly_return':
    #         update_table(param)
    pool = multiprocessing.Pool(processes=settings.PROCESS)
    pool.map(update_table, params)
    pool.close()
    pool.join()
    sync_helper.handle_status_file(schema, [0, 0, 0])
    print("Successfully update schema {}".format(schema))


def check_deleted(schema):
    check_network()
    check_status_file(schema)
    print("Start remove deleted records from schema {}".format(schema))
    sync_helper.handle_status_file(schema, [0, 1, 0])
    tables = fetch_schema_tables(schema)
    params = [{'schema': schema, 'table': k, 'key_columns': v} for k, v in tables.items()]
    # for param in params:
    #     if param['table'] == 'fund_asset_scale':
    #         sync_table(param)
    pool = multiprocessing.Pool(processes=settings.PROCESS)
    pool.map(remove_deleted_records, params)
    pool.close()
    pool.join()
    sync_helper.handle_status_file(schema, [0, 1, 0])
    print("Successfully remove all deleted records from schema {}".format(schema))


def sync_table(schema, table, action):
    tables = fetch_schema_tables(schema)
    if table not in tables.keys():
        raise UpdateError(('StructureError', 'Table {}.{} not exist'.format(schema, table)))
    key_columns = fetch_schema_tables(schema).get(table)
    param = {'schema': schema, 'table': table, 'key_columns': key_columns}
    if action == 'sync':
        update_table(param)
        if settings.DROP_DELETED:
            remove_deleted_records(param)
    elif action == 'update':
        update_table(param)
    elif action == 'remove':
        remove_deleted_records(param)
    elif action == 'check':
        check_table(param)


def check_table(param):
    schema = param.get('schema')
    table_name = param.get('table')
    key_columns = param.get('key_columns')
    _source_engine = create_db_connection(settings.SOURCE_DB_PARAMS, schema)
    _local_engine = create_db_connection(settings.LOCAL_DB_PARAMS, settings.SCHEMA_DICT.get(schema))

    status = {'table_name': table_name, 'start_time': now(), 'status': 'Done'}
    if not check_local_table(table_name, _source_engine, _local_engine):
        structure_changed_error(table_name, status, _local_engine)
        return
    updated_num, deleted_num, check_error, latest_update_time = check_keys_sync(table_name, key_columns, _source_engine,
                                                                                _local_engine)
    status.update({'end_time': now(), 'updated': updated_num, 'deleted': deleted_num,
                   'error_info': check_error, 'latest_update_time': latest_update_time})
    if check_error:
        status.update({'status': 'Break'})
        sync_helper.save_log(table_name, 0, "Break while checking with error {}".format(check_error), 'CHECK-1',
                             _local_engine)
    sync_helper.save_log(table_name, 1, "ALL records updated".format(deleted_num), 'CHECK-1', _local_engine)
    save_sync_status(status, _local_engine)


def update_table(param):
    schema = param.get('schema')
    table_name = param.get('table')
    key_columns = param.get('key_columns')
    _source_engine = create_db_connection(settings.SOURCE_DB_PARAMS, schema)
    _local_engine = create_db_connection(settings.LOCAL_DB_PARAMS, settings.SCHEMA_DICT.get(schema))
    status = {'table_name': table_name, 'start_time': now(), 'status': 'Done'}
    if not check_local_table(table_name, _source_engine, _local_engine):
        structure_changed_error(table_name, status, _local_engine)
        return
    latest_update_time, updated_num, update_error = update_table_by_latest_time(table_name, key_columns,
                                                                                _source_engine, _local_engine)
    status.update({'latest_update_time': latest_update_time, 'updated': updated_num,
                   'error_info': update_error, 'end_time': now()})
    if update_error:
        status.update({'status': 'Break', 'updated': 0})
        sync_helper.save_log(table_name, 0, "Break with updating with error {}".format(update_error), 'UPDATE-1',
                             _local_engine)

        sql = "DELETE FROM {} WHERE update_time > '{}'".format(table_name, latest_update_time)
        _local_engine.execute(sql)
        sync_helper.save_log(table_name, 0, "Rollback with error {}".format(update_error), 'UPDATE-1', _local_engine)

    sync_helper.save_log(table_name, 1, "ALL records updated", 'UPDATE-1', _local_engine)
    save_sync_status(status, _local_engine)


def remove_deleted_records(param):
    schema = param.get('schema')
    table_name = param.get('table')
    key_columns = param.get('key_columns')
    _source_engine = create_db_connection(settings.SOURCE_DB_PARAMS, schema)
    _local_engine = create_db_connection(settings.LOCAL_DB_PARAMS, settings.SCHEMA_DICT.get(schema))

    status = {'table_name': table_name, 'start_time': now(), 'status': 'Done'}
    if not check_local_table(table_name, _source_engine, _local_engine):
        structure_changed_error(table_name, status, _local_engine)
        return
    deleted_num, delete_error, latest_update_time = delete_record(table_name, key_columns, _source_engine,
                                                                  _local_engine)
    status.update({'end_time': now(), 'deleted': deleted_num, 'error_info': delete_error,
                   'latest_update_time': latest_update_time})
    if delete_error:
        status.update({'status': 'Break'})
        sync_helper.save_log(table_name, 0, "Break while deleting with error {}".format(delete_error), 'DELETE-1',
                             _local_engine)
    sync_helper.save_log(table_name, 1, "ALL {} deleted records removed".format(deleted_num), 'DELETE', _local_engine)
    save_sync_status(status, _local_engine)


def update_table_by_latest_time(table_name, key_columns, engine_source, engine_local):
    local_ut = sync_helper.query_local_table_latest_time(table_name, engine_local)

    new_record_num, source_ut = sync_helper.query_new_record_amount(table_name, engine_source, local_ut)
    key_values_generator = sync_helper.main_key_values_generator(table_name, key_columns, engine_source,
                                                                 {'min': local_ut, 'max': source_ut},
                                                                 new_record_num)

    update_jobs = Queue()
    update_results = Queue()
    update_status = Queue()

    threading_update = UpdateByKey(update_jobs, update_results, update_status, table_name, key_columns, engine_source,
                                   engine_local)

    log_start = "{} new records start updating!".format(new_record_num)
    sync_helper.save_log(table_name, 1, log_start, 'UPDATE-1', engine_local)

    threading_update.create_threads()
    threading_update.add_jobs(key_values_generator)

    updated_num, error_status = threading_update.process()
    return local_ut, updated_num, error_status


def check_keys_sync(table_name, key_columns, engine_source, engine_local):
    """
    检查主键，同步并删除数据
    :param table_name: 
    :param key_columns: 
    :param engine_source: 
    :param engine_local: 
    :return: 
    """
    source_ut = sync_helper.query_local_table_latest_time(table_name, engine_source)
    local_ut = sync_helper.query_local_table_latest_time(table_name, engine_local)
    compared_keys_generator = sync_helper.compare_main_keys_generator(table_name, key_columns, engine_source,
                                                                      engine_local, latest=max([source_ut, local_ut]))
    check_jobs = Queue()
    check_results = Queue()
    check_status = Queue()
    threading_check = CompareByKey(check_jobs, check_results, check_status, table_name, key_columns, engine_source,
                                   engine_local)

    log_start = "Start checking!"
    sync_helper.save_log(table_name, 1, log_start, 'CHECK-1', engine_local)
    threading_check.create_threads()
    threading_check.add_jobs(compared_keys_generator)
    updated_num, deleted_num, error_status = threading_check.process()
    return updated_num, deleted_num, error_status, local_ut


def delete_record(table_name, key_columns, engine_source, engine_local):
    source_ut = sync_helper.query_local_table_latest_time(table_name, engine_source)
    local_ut = sync_helper.query_local_table_latest_time(table_name, engine_local)
    compared_keys_generator = sync_helper.compare_main_keys_generator(table_name, key_columns, engine_source,
                                                                      engine_local, latest=max([source_ut, local_ut]))
    delete_jobs = Queue()
    delete_results = Queue()
    delete_status = Queue()
    threading_delete = DeleteByKey(delete_jobs, delete_results, delete_status, table_name, key_columns, engine_local)

    log_start = "Start deleting!"
    sync_helper.save_log(table_name, 1, log_start, 'DELETE-1', engine_local)

    threading_delete.create_threads()
    threading_delete.add_jobs(compared_keys_generator)
    deleted_num, error_status = threading_delete.process()

    return deleted_num, error_status, local_ut


def check_network():
    _source_engine = create_db_connection(settings.SOURCE_DB_PARAMS)
    try:
        pd.read_sql('show databases', _source_engine)
    except:
        raise NoConnection('E001', 'Connection Error, please check your internet connection!')


def check_local_table(table_name, engine_source, engine_local):
    """
    检查本地表结构是否有差异
    :param table_name:
    :param engine_source:
    :param engine_local:
    :return: False 本地表结构有误
    """
    try:
        source_col = set(_query_table_columns(table_name, engine_source))
        target_col = set(_query_table_columns(table_name, engine_local))
        diff = list(source_col.difference(target_col))
        assert len(diff) == 0
        return True
    except (UpdateError, AssertionError):
        return False


def _query_table_columns(table_name, engine):
    try:
        sql = "DESC {}".format(table_name)
        return pd.read_sql(sql, engine)['Field'].tolist()
    except Exception as e:
        raise UpdateError(e)


def structure_changed_error(table_name, status, engine):
    log = "Table structure has changed, please contact us."
    sync_helper.save_log(table_name, 0, log, 'TABLE_STRUCTURE', engine)

    status.update({'updated': 0, 'deleted': 0, 'end_time': now(), 'error_info': log,
                   'status': 'Break'})
    save_sync_status(status, engine)


def save_sync_status(status, engine):
    to_sql('sync_status', engine, pd.DataFrame([status]))


def check_status_file(schema):
    lt_status = sync_helper.status_file_check(schema)
    if lt_status:
        raise UpdateError(('StautsError', lt_status))


def fetch_schema_tables(schema):
    fp = open(os.path.join(settings.DIR_PATH, '{}.json'.format(schema)))
    tables = json.load(fp)
    fp.close()
    return tables


if __name__ == '__main__':
    check_structure()
#     schema = 'product'
#     table = 'fund_info'
#     table_dict = fetch_schema_tables(schema)
#     update_schema(schema)
