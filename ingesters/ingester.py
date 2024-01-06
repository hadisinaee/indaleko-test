import os

class Ingester:
    def __init__(self):
        self.json_file_path=None 
        self.db=None
        self.ingest_method=None
    def set_json_path(self, file_path):
        assert os.path.exists(file_path), f'given file doesn\'exist, got: {file_path}'
        self.json_file_path=file_path

    def set_db(self, db):
        assert db, f'db object is not valid. got: {db}'
        self.db=db 

    def set_ingester_method(self, ingester_method):
        assert ingester_method, f'given ingester is not valid, got: {ingester_method}'
        self.ingest_method=ingester_method
    
    def run(self):
        assert self.ingest_method, f'ingester is None'
        assert self.db, f'db is None'
        assert self.json_file_path, f'json file is None'

        self.ingest_method.run(self.json_file_path, self.db)
    
class IngesterBuilder:
    def __init__(self) -> None:
        self.ingester=Ingester()

    def add_json_file(self, file_path):
        self.ingester.set_json_path(file_path=file_path)
        return self

    def add_db(self, db):
        self.ingester.set_db(db=db)
        return self

    def add_ingester_method(self, ingester):
        self.ingester.set_ingester_method(ingester_method=ingester)
        return self

    def build(self):
        return self.ingester
