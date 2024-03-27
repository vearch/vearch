LIST_DATABASE_URI = "/dbs"
DATABASE_URI = "/dbs/%(database_name)s"  # create or delete database use this uri
LIST_SPACE_URI = "/dbs/%(database_name)s/spaces"
SPACE_URI = '/dbs/%(database_name)s/spaces/%(space_name)s'
UPSERT_DOC_URI = "/document/upsert"
DELETE_DOC_URI = "/document/delete"
SEARCH_DOC_URI = "/document/search"
QUERY_DOC_URI = "/document/query"
INDEX_URI = "/document/index"

AUTH_KEY = "Authorization"

ERR_CODE_SPACE_NOT_EXIST = 565
ERR_CODE_DATABASE_NOT_EXIST = 562

MSG_NOT_EXIST = "notexist"
