Doc Opeartion
=================


Insert
--------

Specify a unique identifier ID when inserting, $doc_id
::

  curl -XPOST -H "content-type: application/json"  -d'
  {
    "area_code":"tpy",
    "product_code":"tpy",
    "image_type":"tpy",
    "image_name":"tpy",
    "image_vec": {
        "source":"test",
        "feature":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    }
  }
  ' http://xxxxxx/$db_name/$space_name/$doc_id


with no id use url: http://xxxxxx/$db_name/$space_name unique ID is automatically generated in the background.


Bulk Insert
--------

::

  curl -XPOST 
  http://xxxxxx/$db_name/$space_name/_bulk


Update
--------

Unique ID must be specified when updating
::

  curl -XPOST -H "content-type: application/json"  -d'
  {
    "area_code":"tpy",
    "product_code":"tpy",
    "image_type":"tpy",
    "image_name":"tpy",
    "image_vec": {
        "source":"test",
        "feature":[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    }
  }
  ' http://xxxxxx/$db_name/$space_name/$doc_id


Delete
--------
::

  curl -XDELETE http://xxxxxx/$db_name/$space_name/$doc_id



Search
--------
