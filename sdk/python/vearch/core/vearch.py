from vearch.config import Config
from vearch.core.db import Database
from vearch.core.client import client
from vearch.schema.space_schema import SpaceScema
from typing import List


class Vearch(object):
    def __init__(self, config: Config):
        self.client = client
        self.client.config(config)

    def create_database(self, database_name: str) -> None:
        self.client._create_db(database_name)

    def list_databases(self) -> List[Database]:
        ## TODO lsit database
        return List[Database("test")]

    def database(self, database_name: str) -> Database:
        return Database(database_name)

    def drop_database(self, database_name: str) -> None:
        self.client._drop_db(database_name)

    def create_space(self, database_name: str, space_name: str, space: SpaceScema) -> None:
        pass
