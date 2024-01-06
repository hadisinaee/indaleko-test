from decimal import Decimal
import json
import ijson
import uuid

from db import dbhandler as dbh

class DecimalEncoder(json.JSONEncoder):
    """
    deals with Decimal objects in ijson parsing results
    """
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

class OnDiskIngester:
    def __init__(self,username, password, database, container_name, debug=False) -> None:
        self.user_name=username
        self.password=password
        self.database=database
        self.container_name=container_name
        self.debug=debug
        self.__dest_folderpath__='/home/'

        self.uuids = {} # path -> uuid4; keep track of the uuids of objects

        self.written_vertices=0 # keep track of the number of vertices written sofar
        self.written_edges=0 # keep track of the number of edges written sofar(both contanis and contained_by counts as 1)

        self.vertices_filepath="./data/vertices.json" # file path where we save our vertices json
        self.vertices_file=open(self.vertices_filepath, 'w')
        self.written_roots=set() # keep track of the root objects we have 

        self.contains_edge_filepath="./data/contains_edge.json"
        self.contains_edge_file=open(self.contains_edge_filepath, 'w')

        self.contained_by_edge_filepath="./data/contained_by_edge.json"
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
        self.debug and print('wrote', obj['url'])

        self.written_vertices+=1

    def run(self, indexed_file_path: str, _: dbh.IndalekoDB):
        print(f'ingesting ({indexed_file_path})')

        with open(indexed_file_path, 'rb') as input_file:
            items = ijson.items(input_file, 'item')
            
            for item in items:
                
                # get the uuid of the parent and the child
                root, child=item['path'], item['file']
                root_uuid=self.__make_a_vertice__(root)
                child_uuid=self.__make_a_vertice__('{}:{}'.format(root_uuid, child))

                self.debug and print(f'root={root} child={child} is {'dir' if item['is_dir'] else 'file'}')

                # add the root folder (searched from in the index) to the vertices
                if root_uuid not in self.written_roots:
                    if self.written_vertices!=0: self.vertices_file.write(',\n')
                    self.__add_vertices__(root_uuid, {
                        'file': item['path'],
                        'is_dir': True,
                        'st_dev': 'dummy',
                        'st_ino': -1,
                        'URI': item['path']
                    })
                    self.written_roots.add(root_uuid)

                # add the child to the vertices
                if self.written_vertices == 0: print('num written vertices == 0')
                if self.written_vertices!=0: self.vertices_file.write(',\n')
                self.__add_vertices__(child_uuid, item)

                # add the edge (parent, child)
                if self.written_edges!=0:
                    self.contains_edge_file.write(',\n')
                    self.contained_by_edge_file.write(',\n')
                self.__add_edge__(root_uuid, child_uuid)

                
                self.debug and print('-'*10)
            self.vertices_file.write('\n]')
            self.contains_edge_file.write('\n]')
            self.contained_by_edge_file.write('\n]')
        
        self.contained_by_edge_file.flush()
        self.contains_edge_file.flush()
        self.vertices_file.flush()

        self.__bulkimport__()
    
    
    def __bulkimport__(self):
        import subprocess
        import os
        from functools import partial

        run_partial=partial(
            subprocess.run, 
            stdout=subprocess.PIPE, 
            stderr=subprocess.PIPE, 
            text=True)


        # copy the json files to the home folder in the docker
        # e.g. cp_cmd=f'docker cp [json-file-path] [container-name/id]:[dest-path]'
        cp_cmd_tmpl='docker cp {filepath} {container}:{dest_path}'

        file_paths=map(os.path.abspath, [self.vertices_filepath, self.contains_edge_filepath, self.contained_by_edge_filepath])
        cp_cmds= [ cp_cmd_tmpl.format(filepath=f, container=self.container_name,dest_path= self.__dest_folderpath__ ) for f in file_paths ]
        

        try:
            print(f'copying json files (vertices and edges) to the container at {self.__dest_folderpath__} ...')

            for cmd_result in map(lambda cmd: run_partial(cmd.split(' ')), cp_cmds):
                if cmd_result.returncode!=0:
                    print(f'failed to copy the file to the container, file={''.join(cmd_result.args)}')
                    print(f'stdout: {cmd_result.stdout}')
                    print(f'stderr: {cmd_result.stderr}')
                    return

            print(f'copying is done!')            

            print(f'importing vertices')


        except subprocess.CalledProcessError as e:
            print(f'Couldn\'t copy the results to the docker: {e}')

        # importing files to ADB 
        import_paths=map(lambda f: os.path.join(self.__dest_folderpath__, f), ['vertices.json', 'contains_edge.json', 'contained_by_edge.json'])
        collections=["DataObjects", "contains", "contained_by"]
        
        # run arangoimport --file ./contains_edge.json --type "json" --collection "contains"  --server.u sername "Hf7t17XL" --server.password "D4rL6jFk167MOZx" --server.database "Indaleko" --create-collection true  --overwrite true --to-collection-prefix "DataObjects" --from-collection-prefix "DataObjects"
        imprt_cmd_tmpl = 'docker exec -t {container} arangoimport --batch-size 1GB --file {file_path} --type json --collection {collection} --server.username {username} --server.password {password} --server.database {database} --create-collection true --overwrite true --to-collection-prefix {to_collection} --from-collection-prefix {from_collection}'

        imprt_cmds = [imprt_cmd_tmpl.format(container=self.container_name,file_path=file, collection=collection, username=self.user_name, password=self.password, database=self.database, to_collection=collections[0], from_collection=collections[0]) for file, collection in zip(import_paths, collections)]

        try:
            print('importing files to ADB')
            for res in map(lambda f: run_partial(f.split(' ')), imprt_cmds):
                print(f'retcode:{res.returncode}, stdout: {res.stdout}')
                if res.returncode!=0:
                    print(f'failed at: {' '.join(res.args)}')
                    print('err:', res.stderr)
        except subprocess.CalledProcessError as e:
            print(f'Error while import files to ADB: {e}')