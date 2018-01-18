from sqlalchemy import create_engine


def create_db_connection(params, schema=None):
    _db_params = 'mysql+pymysql://{user}:{passwd}@{host}:{port}'.format(**params)
    if schema:
        _db_params += '/{}'.format(schema)
    return create_engine(_db_params, connect_args={"charset": "utf8"}, pool_size=0)
