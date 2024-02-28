from vearch.config import Config
from vearch.core.db import Database
from vearch.core.vearch import Vearch

if __name__=="__main__":
    config = Config(host="http://127.0.0.1")
    vc = Vearch(config)
    print(vc.client.host)
    vc.create_database("database1")
    db = Database(name="fjakjfks")
    db.create()
