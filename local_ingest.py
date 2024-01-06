import configparser
import os
import configparser

from db import dbhandler as dbh
from ingesters import ingester, methods
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingester CLI')

    parser.add_argument('--config', '-c', dest='config_path',required=True, help='Path to the config file')
    parser.add_argument('--indexed-file-path', '-i' ,dest='indexed_file_path', required=True, help='Path to the indexed file')
    parser.add_argument('--debug', '-d', dest='debug',action='store_true', help='Print debug logs')

    args=parser.parse_args()


    # config_path='/Users/sinaee/Projects/indaleko-test/config/indaleko-db-config.ini'
    assert os.path.exists(args.config_path), f'file doesn\'t exist: {args.config_path}'

    # indexed_file_path='/Users/sinaee/Projects/indaleko-test/data/mac.json'
    assert os.path.exists(args.indexed_file_path), f'the indexed file doesn\'t exist, got: {args.indexed_file_path}'
    
    DB_SECTION='database'
    config = configparser.ConfigParser()
    config.read(args.config_path)

    idb = dbh.IndalekoDB(
        hostname=config.get(DB_SECTION,'host'),
        port=config.get(DB_SECTION, 'port'),
        username=config.get(DB_SECTION, 'user_name'),
        password=config.get(DB_SECTION, 'user_password')
               )
    idb.connect(config.get(DB_SECTION, 'database'))
    idb.setup_collections(dbh.Indaleko_Collections, reset=True)

    file_ingester=ingester.IngesterBuilder() \
    .add_json_file(args.indexed_file_path) \
    .add_db(idb) \
    .add_ingester_method(methods.OnDiskIngester(
        username=config.get(DB_SECTION, 'user_name'),
        password=config.get(DB_SECTION, 'user_password'),
        database=config.get(DB_SECTION, 'database'),
        container_name=config[DB_SECTION]['container'],
        debug=args.debug))\
    .build()

    file_ingester.run()