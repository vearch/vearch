from vearch.schema.index import Index


class Space(object):
    def __int__(self, db_name, name):
        self.db_name = db_name
        self.name = name

    def create(self):
        pass

    def create_index(self, field: str, index: Index):
        pass
