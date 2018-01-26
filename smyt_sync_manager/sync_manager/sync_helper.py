from smyt_sync_manager.config import settings
import pandas as pd
from smyt_sync_manager.utils import to_sql, now
import os
import json
import threading


def fetch_main_key_values(table_name, key_columns, engine, latest=None, limit=None):
    """
    取表主键的值
    :param table_name:
    :param key_columns:
    :param engine:
    :param latest:
    :param limit:
    :return:
    """
    sql = "SELECT CONCAT_WS('[=]',{mks}) as `key_columns` FROM {tb}".format(tb=table_name, mks=','.join(
        ["IFNULL({}, 'NULL_VALUE')".format(_) for _ in key_columns]))
    if latest:
        if isinstance(latest, dict):
            sql += " WHERE update_time > '{min}' and update_time <= '{max}'".format(**latest)
        else:
            sql += " WHERE update_time > '{}' ORDER BY update_time".format(latest)
    if limit:
        sql += " LIMIT {}, {}".format(*limit)
    mks = pd.read_sql(sql, engine)['key_columns'].tolist()
    return mks


def query_new_record_amount(table_name, engine, latest):
    """
    查询新增记录总量
    :param table_name:
    :param engine:
    :param latest:
    :return:
    """
    sql_num = "SELECT COUNT(*) as `num`, ifnull(DATE_FORMAT(max(update_time), '%%Y-%%m-%%d %%H:%%i:%%s'), " \
              "'{}' ) as msd FROM {} WHERE update_time > '{}'".format(latest, table_name, latest)
    result = pd.read_sql(sql_num, engine)
    num = result['num'][0]
    msd = result['msd'][0]
    return num, msd


def main_key_values_generator(table_name, key_columns, engine, latest, num):
    for i in range(0, num, settings.LIMIT_RECORDS):
        chunksize = min(settings.LIMIT_RECORDS, num - i + 1)
        mks = fetch_main_key_values(table_name, key_columns, engine, latest, limit=(i, chunksize))
        mks = [_.split('[=]') for _ in mks]
        yield mks


def query_local_table_latest_time(table_name, local_engine):
    sql = "SELECT ifnull( DATE_FORMAT(max(update_time), '%%Y-%%m-%%d %%H:%%i:%%s'), '1970-01-01 00:00:01' ) " \
          "as `max` FROM {}".format(table_name)
    return pd.read_sql(sql, local_engine)['max'][0]


def update_by_keys(table_name, key_columns, key_values, source_engine, local_engine):
    if not len(key_values):
        return 0
    sql = "SELECT * FROM {tb} WHERE {mkv}".format(tb=table_name, mkv=_format_sql(key_columns, key_values))
    insert_records = pd.read_sql(sql, source_engine)
    nums = len(insert_records)
    if nums:
        print("{}: {} -- {} new records updating!\n".format(now(), table_name, nums))
        # noinspection PyBroadException
        try:
            to_sql(table_name, local_engine, insert_records)
        except Exception:
            to_sql(table_name, local_engine, insert_records, chunksize=10)
        log = '{} records updated!'.format(nums)
        save_log(table_name, 1, log, 'UPDATE-0', local_engine)
    return nums


def delete_by_keys(table, key_columns, key_values, engine):
    nums = len(key_values)
    if nums:
        print("{}: {} -- {} records deleting!\n".format(
            now(), table, nums))
        sql = "DELETE FROM {} WHERE {}".format(table, _format_sql(key_columns, key_values))
        engine.execute(sql)
        _deleted = pd.DataFrame(key_values, columns=key_columns)
        _deleted['table_name'] = table
        to_sql('sync_deleted_keys', engine, _deleted)
        log = "{} records deleted!".format(nums)
        save_log(table, 1, log, 'DELETE-0', engine)
    return nums


def joined_main_key_values(table_name, key_columns, source_engine, local_engine, latest):
    main_key = key_columns[0]
    source_main_key = fetch_unique_main_key(table_name, main_key, source_engine, latest)
    local_main_key = fetch_unique_main_key(table_name, main_key, local_engine, latest)
    main_key_values = list(set(source_main_key).union(set(local_main_key)))
    return main_key_values


def compare_main_keys_generator(table_name, key_columns, source_engine, local_engine, latest=None):
    main_key_values = joined_main_key_values(table_name, key_columns, source_engine, local_engine, latest)
    mk_num = len(main_key_values)
    for i in range(0, mk_num, settings.MAIN_KEY_LIMIT):
        mkv = main_key_values[i: min(mk_num, settings.MAIN_KEY_LIMIT + i)]
        yield compare_main_keys(table_name, mkv, key_columns, source_engine, local_engine, latest)


def compare_main_keys(table_name, main_key_values, key_columns, source_engine, local_engine, latest):
    source_keys = fetch_distinct_keys(table_name, main_key_values, key_columns, source_engine, latest)
    local_keys = fetch_distinct_keys(table_name, main_key_values, key_columns, local_engine, latest)
    compared = local_keys.merge(source_keys, on=['key_columns'], suffixes=['_t', '_s'], how='outer')
    compared.update_time_t = compared.update_time_t.fillna(pd.Timestamp(1970, 1, 1, 0, 0, 1))
    deleted = compared[compared.update_time_s.isnull()]['key_columns'].apply(lambda x: x.split('[=]')).tolist()
    updated = compared[compared.update_time_s > compared.update_time_t]['key_columns'].apply(
        lambda x: x.split('[=]')).tolist()
    threading.Thread(target=save_check_result, args=(table_name, key_columns, deleted, updated, local_engine,)).start()
    return deleted, updated


def fetch_distinct_keys(table, main_key_values, key_columns, engine, latest=None):
    sql = "SELECT CONCAT_WS('[=]',{mks}) as `key_columns`, update_time FROM {tb} " \
          "WHERE {v}".format(mk=key_columns[0], tb=table,
                             mks=','.join(["IFNULL({}, 'NULL_VALUE')".format(_) for _ in key_columns]),
                             v=_format_list_value(key_columns[0], main_key_values))
    if latest:
        sql += " AND update_time <= '{}'".format(latest)
    return pd.read_sql(sql, engine)


def fetch_unique_main_key(table, main_key, engine, latest):
    mk = main_key
    if main_key in ('statistic_date', 'statistic_date_std'):
        mk = "DATE_FORMAT({}, '%%Y-%%m-%%d') as {}".format(main_key, main_key)
    sql = "SELECT DISTINCT {} FROM {} WHERE update_time <= '{}' ORDER BY {}".format(mk, table, latest, main_key)
    return pd.read_sql(sql, engine)[main_key].tolist()


def _format_sql(columns, values):
    return ' OR '.join(
        ['({})'.format(
            ' AND '.join(["{} = '{}'".format(k, v) for k, v in zip(columns, _) if v != settings.IFNULL_VALUE])) for _
            in
            values])


def _format_list_value(columns, values):
    string = []

    if None in values:
        values.remove(None)
        string.append(" {} is NULL ".format(columns))
    if len(values):
        string.append(" {}".format("{} IN ({})".format(columns, ','.join(["'{}'".format(_) for _ in values]))))
    return ' OR '.join(string)


def save_log(table_name, status, log, level, engine):
    print("{}: {} -- {}\n".format(now(), table_name, log))
    log_record = pd.DataFrame([[table_name, status, log, level]], columns=['table_name', 'status', 'log_info', 'level'])
    to_sql('sync_log', engine, log_record)


def save_check_result(table_name, key_columns, deleted, updated, local_engine):
    if deleted:
        record = pd.DataFrame(deleted, columns=key_columns)
        record['action'] = 'delete'
        record['table_name'] = table_name
        to_sql('sync_checked_keys', local_engine, record)
    if updated:
        record = pd.DataFrame(updated, columns=key_columns)
        record['action'] = 'update'
        record['table_name'] = table_name
        to_sql('sync_checked_keys', local_engine, record)


def status_file_check(schema):
    file_path = "{}/{}_status.json".format(settings.DIR_PATH, schema)
    if not os.path.exists(file_path):
        return
    with open(file_path) as fp:
        sf = json.load(fp)
        error = []
        for a, s in sf.items():
            if s == 1:
                error.append(a)
        if error:
            return "Schema {} last time action: {} did not " \
                   "finished yet or interrupted!".format(schema, ','.join(error))


def handle_status_file(schema, status):
    actions = ['UPDATE', 'DELETE', 'CHECK']
    status = dict(zip(actions, status))
    with open("{}/{}_status.json".format(settings.DIR_PATH, schema), 'w+') as fp:
        fp.write(str(status).replace('\'', '"'))

# if __name__ == '__main__':
#     from config.settings import create_db_connection, SOURCE_DB_PARAMS, LOCAL_DB_PARAMS, SCHEMA_DICT, DIR_PATH
#     import json, os
# 
#     schema = 'product'
#     table = 'fund_info'
#     table_dict = json.load(open(os.path.join(DIR_PATH, '{}.json'.format(schema))))
#     _source_engine = create_db_connection(SOURCE_DB_PARAMS, schema)
#     _local_engine = create_db_connection(LOCAL_DB_PARAMS, SCHEMA_DICT.get(schema))
#     f = compare_main_keys_generator(table, table_dict.get(table), _source_engine, _local_engine)
#     for i in f:
#         if i[0]:
#             print(i)
