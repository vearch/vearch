Database Operation
=================

List Database
--------

::

   curl -XGET http://xxxxxx/list/db


Create Database
--------

::

   curl -XPUT -H "content-type:application/json" -d '{
     "name": "db_name"
   }' http://xxxxxx/db/_create


View Database
--------

::

   curl -XGET http://xxxxxx/db/$db_name


Delete Database
--------

::

   curl -XDELETE http://xxxxxx/db/$db_name


View Database Space
--------

::

   curl -XGET http://xxxxxx/list/space?db=$db_name



