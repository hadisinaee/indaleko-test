import configparser
from decimal import Decimal
import os
import json
from typing import Any
import uuid
import stat
from arango import ArangoClient
import arango
import datetime
import configparser
import ijson

class IndalekoDB:

    def __init__(self, hostname='localhost', port=8529, username='root', password = None):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password

        self.database = None # the database name we connect to
        self.collections =None # a dictionary of name:IndalekoCollection; 

        self.client = None # ADB client obj
        self.db = None # ADB database obj we have connected to

    def connect(self, database):
        assert database, f'the database name is invalid, got: {database}'
        adb_url='http://{}:{}'.format(self.hostname, self.port)
        if not self.client:
            try:
                self.client = ArangoClient(hosts=adb_url)

                self.db=self.client.db(
                    name=database,
                    username=self.username,
                    password=self.password,
                    auth_method='basic'
                )

                self.database=database
                print(f"Connected to database: {database} @ {adb_url}")
            except Exception as e:
                print(f"Error connecting to the database: {e}")

    def setup_collections(self,collection_names, reset=False) -> dict:
        collections = {}
        for name in collection_names:
            assert name not in collections, 'Duplicate collection name'
            edge = False
            if 'edge' in collection_names[name]:
                assert type(collection_names[name]['edge']) is bool
                edge = collection_names[name]['edge']
            # I commented the next line because it doesn't make sense according to the previous if-clause
            # edge = False
            collections[name] = IndalekoCollection(self.db, name, edge, reset)
            if 'indices' in collection_names[name]:
                for index in collection_names[name]['indices']:
                    e = collection_names[name]['indices'][index]
                    assert e['type'] == 'persistent', 'Only support persistent'
                    collections[name].create_index(index, e['type'], e['fields'], e['unique'])
        self.collections=collections

    def __str__(self) -> str:
        return f'ADB: {self.hostname}:{self.port}  DB={self.database}  user={self.username} password={self.password[:2]+'*'*4}'



class ContainerRelationship:

    ContainsRelationshipSchema = {
        '_from_field' : {
            'type' : 'string',
            'rule' : {'type', 'uuid'}
        },
        '_to_field' : {
            'type' : 'string',
            'rule' : {'type', 'uuid'}
        }
    }

    def __init__(self, db, start, end, collection):
        self._from = start
        self._to = end
        db[collection].insert(self._dict_)

    def to_json(self):
        return json.dumps(self.__dict__)

class IndalekoIndex:
    def __init__(self, collection: 'IndalekoCollection', index_type: str, fields: list, unique=False):
        self.collection = collection
        self.fields = fields
        self.unique = unique
        self.index_type = index_type
        assert index_type == 'persistent', 'Only support persistent indices'
        self.index = self.collection.add_persistent_index(fields=self.fields, unique=self.unique)

    def find_entries(self, **kwargs):
        return [document for document in self.collection.find(kwargs)]


class IndalekoCollection:

    def __init__(self, db, name: str, edge: bool = False, reset: bool = False) -> None:
        self.db = db
        self.name = name
        if reset and db.has_collection(name):
            db.delete_collection(name)
        if not db.has_collection(name):
            db.create_collection(name, edge=edge)
        self.collection = db.collection(self.name)
        self.indices = {}

    def create_index(self, name: str, index_type: str, fields: list, unique: bool) -> 'IndalekoCollection':
        self.indices[name] = IndalekoIndex(self.collection, index_type, fields, unique)
        return self

    def find_entries(self, **kwargs):
        return [document for document in self.collection.find(kwargs)]

    def insert(self, document: dict) -> 'IndalekoCollection':
        return self.collection.insert(document)

class FileSystemObject:
    ObjectCount = 0
    RelationshipCount = 0

    DataObjectSchema = {
        'url_field': {
            'type': 'string',
            'rule': {'type': 'url'}
        },
        'uuid_field': {
            'type': 'string',
            'rule': {'type': 'uuid'}
        },
        # Define other fields in the schema
    }

    def __init__(self, collection: 'IndalekoCollection', path : str, root=False):
        self.root = root
        self.uuid = str(uuid.uuid4())
        self.url = 'file:///' + path
        self.stat_info = os.stat(path)
        self.size = self.stat_info.st_size
        self.timestamps = {
            'created': datetime.datetime.fromtimestamp(self.stat_info.st_ctime).isoformat(),
            'modified': datetime.datetime.fromtimestamp(self.stat_info.st_mtime).isoformat(),
            'accessed': datetime.datetime.fromtimestamp(self.stat_info.st_atime).isoformat(),
        }
        self.collection = collection
        '''Note: this is much faster than catching the exception and then doing
        the lookup, at least in the case where there are a lot of collisions.'''
        try:
            self.dbinfo = collection.insert(self.to_dict())
        except arango.exceptions.DocumentInsertError as e:
            documents = collection.find_entries(dev=self.stat_info.st_dev,
                                            inode=self.stat_info.st_ino)
            if len(documents) > 0:
                self.dbinfo = documents[0]
            else:
                print('Exception {} on file {}'.format(e, path))
                documents = collection.find_entries(url=self.url)
                if len(documents) > 0:
                    self.dbinfo = documents[0]
                else:
                    raise e
        FileSystemObject.ObjectCount += 1


    def add_contain_relationship(self, collections: dict, child_obj: 'FileSystemObject') -> 'FileSystemObject':
        assert stat.S_ISDIR(self.stat_info.st_mode), 'Should only add contain relationships from directories'
        parent_id = self.dbinfo['_id']
        child_id = child_obj.dbinfo['_id']
        collections['contains'].insert(json.dumps({'_from': parent_id, '_to': child_id, 'uuid1' : self.uuid, 'uuid2' : child_obj.uuid}))
        collections['contained_by'].insert(json.dumps({'_from': child_id, '_to': parent_id, 'uuid1' : child_obj.uuid, 'uuid2' : self.uuid}))
        FileSystemObject.RelationshipCount += 2
        return self

    def windows_attributes_to_data(self):
        attributes = self.stat_info.st_file_attributes
        data = {}
        prefix = 'FILE_ATTRIBUTE_'
        for attr in dir(stat):
            if attr.startswith(prefix):
                element_name = attr[len(prefix):]
                if attributes & getattr(stat, attr):
                    data[element_name] = True
                else:
                    data[element_name] = False
        return data

    def posix_attributes_to_data(self):
        attributes = self.stat_info.st_mode
        data = {}
        prefix = 'S_IS'
        for attr in dir(stat):
            if attr.startswith(prefix) and callable(getattr(stat,attr)):
                element_name = attr[len(prefix):]
                if getattr(stat,attr)(attributes):
                    data[element_name] = True
                else:
                    data[element_name] = False
        return data

    def to_dict(self):
        data = {
            'url': self.url,
            'timestamps': {
                'created': datetime.datetime.fromtimestamp(self.stat_info.st_ctime).isoformat(),
                'modified': datetime.datetime.fromtimestamp(self.stat_info.st_mtime).isoformat(),
                'accessed': datetime.datetime.fromtimestamp(self.stat_info.st_atime).isoformat(),
            },
            'size': self.size,
            'mode': self.stat_info.st_mode,
            'posix attributes' : self.posix_attributes_to_data(),
            'dev' : self.stat_info.st_dev,
            'inode' : self.stat_info.st_ino
        }
        if hasattr(self.stat_info, 'st_file_attributes'):
            # windows only
            data['Windows Attributes'] = self.windows_attributes_to_data()
        return json.dumps(data)

  
Indaleko_Collections = {
        'DataObjects': {
            'schema' : FileSystemObject.DataObjectSchema,
            'edge' : False,
            'indices' : {
                'url' : {
                    'fields' : ['url'],
                    'unique' : True,
                    'type' : 'persistent'
                },
                'file identity' : {
                    'fields' : ['dev', 'inode'],
                    'unique' : True,
                    'type' : 'persistent'
                },
            },
        },
        'contains' : {
            'schema' : ContainerRelationship.ContainsRelationshipSchema,
            'edge' : True,
            'indices' : {
                'container' : {
                    'fields' : ['uuid1', 'uuid2'],
                    'unique' : True,
                    'type' : 'persistent',
                }
            }
        },
        'contained_by' : {
            'schema' : ContainerRelationship.ContainsRelationshipSchema,
            'edge' : True,
            'indices' : {
                'contained_by' : {
                    'fields' : ['uuid1', 'uuid2'],
                    'unique' : True,
                    'type' : 'persistent',
                },
            },
        },
}

class DecimalEncoder(json.JSONEncoder):
    """
    deals with Decimal objects in ijson parsing results
    """
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)
    
class UnixLocalIngest():
    def __init__(self) -> None:
        self.uuids = {} # path -> uuid4; keep track of the uuids of objects

        self.written_vertices=0 # keep track of the number of vertices written sofar
        self.written_edges=0 # keep track of the number of edges written sofar(both contanis and contained_by counts as 1)

        self.vertices_filepath="data/vertices.json" # file path where we save our vertices json
        self.vertices_file=open(self.vertices_filepath, 'w')
        self.written_roots=set() # keep track of the root objects we have 

        self.contains_edge_filepath="data/contains_edge.json"
        self.contains_edge_file=open(self.contains_edge_filepath, 'w')

        self.contained_by_edge_filepath="data/contained_by_edge.json"
        self.contained_by_edge_file=open(self.contained_by_edge_filepath, 'w')

    
    def __add_edge__(self, parent, child):
        if self.written_edges==0: 
            self.contained_by_edge_file.write('[\n')
            self.contains_edge_file.write('[\n')
        
        # TODO: we don't need `edge` here because we are going to add to the edge collection(?)
        json.dump(
            {
                "_from": parent,
                "_to": child,
                "uuid1": parent,
                "uuid2": child
            },
            self.contains_edge_file,
            indent=2
        )
        json.dump(
            {
                "_from": child,
                "_to": parent,
                "uuid1": child,
                "uuid2": parent,
                "edge": True
            },
            self.contained_by_edge_file,
            indent=2
        )
        self.written_edges+=1

    def __make_a_vertice__(self, path) -> str:
        """
        creates a new UUID for the given `path` and cache it
        """
        if path not in self.uuids:
            self.uuids[path]=str(uuid.uuid4())
        return self.uuids[path]

    def __add_vertices__(self, fs_obj_uuid, json_obj):
        """
        append given uuids in  obj_uuid_list to the vertices.json file. Use json_obj from index file
        """
        if self.written_vertices == 0: self.vertices_file.write('[\n')
        obj={
            "_key": fs_obj_uuid,
            "name": json_obj['file'],
            "is_dir": json_obj['is_dir'],
            "dev": json_obj['st_dev'],
            "inode": json_obj['st_ino'],
            "url": json_obj['URI'],
            "meta_data": None
        }
        meta_data={}
        for k in [ meta_key for meta_key in json_obj if meta_key.startswith('st_')]:
            meta_data[k] = json_obj[k]
        obj['meta_data']=meta_data

        json.dump(obj, self.vertices_file,cls=DecimalEncoder, indent=2)
        print('wrote', obj['url'])

        self.written_vertices+=1

    def ingest(self, indexed_file_path: str, idb: IndalekoDB):
        print(f'ingesting ({indexed_file_path}) using ({idb})')

        with open(indexed_file_path, 'rb') as input_file:
            items = ijson.items(input_file, 'item')
            
            for item in items:
                
                # get the uuid of the parent and the child
                root, child=item['path'], item['file']
                root_uuid=self.__make_a_vertice__(root)
                child_uuid=self.__make_a_vertice__('{}:{}'.format(root_uuid, child))
                print(f'root={root} child={child} is {'dir' if item['is_dir'] else 'file'}')

                # add the root folder (searched from in the index) to the vertices
                if root_uuid not in self.written_roots:
                    self.__add_vertices__(root_uuid, {
                        'file': item['path'],
                        'is_dir': True,
                        'st_dev': 'dummy',
                        'st_ino': -1,
                        'URI': item['path']
                    })
                    self.written_roots.add(root_uuid)

                # add the child to the vertices
                if self.written_vertices!=0: self.vertices_file.write(',\n')
                self.__add_vertices__(child_uuid, item)

                # add the edge (parent, child)
                if self.written_edges!=0:
                    self.contains_edge_file.write(',\n')
                    self.contained_by_edge_file.write(',\n')
                self.__add_edge__(root_uuid, child_uuid)

                
                print('-'*10)
            self.vertices_file.write('\n]')
            self.contains_edge_file.write('\n]')
            self.contained_by_edge_file.write('\n]')

            # for k,v in self.uuids.items():
            #     print(k, '->', v)


if __name__ == '__main__':
    config_path='/Users/sinaee/Projects/indaleko-test/config/indaleko-db-config.ini'
    assert os.path.exists(config_path), f'file doesn\'t exist: {config_path}'

    indexed_file_path='/Users/sinaee/Projects/indaleko-test/data/mac.json'
    assert os.path.exists(indexed_file_path), f'the indexed file doesn\'t exist, got: {indexed_file_path}'
    
    DB_SECTION='database'
    config = configparser.ConfigParser()
    config.read(config_path)

    idb = IndalekoDB(
        hostname=config.get(DB_SECTION,'host'),
        port=config.get(DB_SECTION, 'port'),
        username=config.get(DB_SECTION, 'user_name'),
        password=config.get(DB_SECTION, 'user_password')
               )
    idb.connect(config.get(DB_SECTION, 'database'))
    idb.setup_collections(Indaleko_Collections, reset=True)

    uli = UnixLocalIngest()
    uli.ingest(indexed_file_path, idb)
    