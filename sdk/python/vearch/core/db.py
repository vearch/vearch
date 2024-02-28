from vearch.core.space import Space
from typing import List
from vearch.core.client import client


class Database(object):
    def __init__(self, name):
        self.name = name
        self.client = client

    def exist(self) -> bool:
        ## TODO check database exist
        return True

    def create(self):
        self.client._create_database(self.name)

    def drop(self):
        self.client._drop_db(self.name)

    def list_spaces(self) -> List[Space]:
        return [Space("fdasfs", "fdsafsd"), ]

    def space(self, name) -> Space:
        ## TODO check space exists
        return Space(self.name, name)

    def create_space(self):
        pass
