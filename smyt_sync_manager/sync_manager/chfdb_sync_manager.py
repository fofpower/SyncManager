from .sync import sync_chfdb, check_chfdb, update_schema, check_schema, check_deleted, sync_schema, \
    sync_table, check_schema_structure, check_structure
from smyt_sync_manager.config import settings
import os
import argparse
from importlib import reload


def handle_commandline():
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--action", type=str, default='sync',
                        help="sync, update, remove, check, structure")
    parser.add_argument("-s", "--schema", type=str, default=None,
                        help="{}".format(', '.join(settings.SCHEMA_DICT.keys())))
    parser.add_argument("-t", "--table", type=str, default=None,
                        help="specify a table to sync")
    parser.add_argument("-d", "--drop_deleted", type=str, default='off',
                        help="remove deleted keys or not, 'on' or 'off'")
    parser.add_argument("-kl", "--key_limit", type=int, default=None,
                        help="unique main key amount chose to compare each time")
    parser.add_argument("-rl", "--record_limit", type=int, default=None,
                        help="record amount chose to insert each time")
    parser.add_argument("-c", "--concurrency", type=int, default=None,
                        help="how many sub threads run for one processing")
    parser.add_argument("-p", "--processes", type=int, default=None,
                        help="how many processes run at the same time")

    args = parser.parse_args()
    return args.action, args.schema, args.drop_deleted, args.table, args.key_limit, \
           args.record_limit, args.concurrency, args.processes


def main():
    action, schema, drop_deleted, table, key_limit, record_limit, concurrency, processes = handle_commandline()
    os.environ['CHFDB_SYNC_DROP_DELETED'] = drop_deleted
    reload(settings)
    if key_limit:
        settings.MAIN_KEY_LIMIT = key_limit
    if record_limit:
        settings.LIMIT_RECORDS = record_limit
    if concurrency:
        settings.CONCURRENCY = concurrency
    if processes:
        settings.PROCESS = processes
    if action == 'sync':
        if schema:
            if table:
                sync_table(schema, table, action)
            else:
                sync_schema(schema)
        else:
            sync_chfdb()

    elif action == 'check':
        if schema:
            if table:
                sync_table(schema, table, action)
            else:
                check_schema(schema)
        else:
            check_chfdb()

    elif action == 'update' and schema:
        if table:
            sync_table(schema, table, action)
        else:
            update_schema(schema)
    elif action == 'remove' and schema:
        if table:
            sync_table(schema, table, action)
        else:
            check_deleted(schema)
    elif action == 'structure':
        if schema:
            check_schema_structure(schema)
        else:
            check_structure()
    else:
        raise KeyError(
            'Args (schema: {}, table: {}, action: {}, drop_deleted: {}) not supported'.format(schema, table, action,
                                                                                              drop_deleted))
    os.environ['CHFDB_SYNC_DROP_DELETED'] = 'off'


if __name__ == '__main__':
    main()
