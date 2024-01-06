
import os
import json
import uuid
import stat
from arango import ArangoClient
import arango
import datetime

class IndalekoDB:
    def __init__(self, hostname='localhost', port=8529, username='root', password = None):
        self.hostname = hostname
        self.port = port
        self.username = username
        self.password = password

        self.database = None # the database name we connect to
        self.collections =None # a dictionary of name:IndalekoCollection; 

        self.client = None # ADB client obj
        self.db = None # ADB database obj we have to connected to

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
