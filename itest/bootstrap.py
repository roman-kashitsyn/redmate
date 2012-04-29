import os
from ConfigParser import ConfigParser

def _file_path(file_name):
    base_path = os.path.dirname(__file__)
    return os.path.abspath(os.path.join(base_path, file_name))

def read_config():
    conf = ConfigParser()
    conf.read(_file_path('config.ini'))
    return conf

def init_db(db):
    statements = open(_file_path('bootstrap.sql'), 'r').read()
    cursor = db.cursor()
    for statement in statements.split(';'):
        if statement.strip():
            cursor.execute(statement)
    db.commit()
