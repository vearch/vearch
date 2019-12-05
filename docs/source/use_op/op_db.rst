Database Operation
=================

http://master_server is the master service, $db_name is the name of the created database.

List Database
--------

::

   curl -XGET http://master_server/list/db


Create Database
--------

::

   curl -XPUT -H "content-type:application/json" -d '{
     "name": "db_name"
   }' http://master_server/db/_create


View Database
--------

::

   curl -XGET http://master_server/db/$db_name


Delete Database
--------

::

   curl -XDELETE http://master_server/db/$db_name

Cannot delete if there is a table space under the datebase.

View Database Space
--------

::

   curl -XGET http://master_server/list/space?db=$db_name



