Cluster Monitoring
=================

http://master_server is the master service.

Cluster Status
--------

::

   curl -XGET http://master_server/_cluster/stats


Health Status
--------

::

   curl -XGET http://master_server/_cluster/health


Port Status
--------

::

   curl -XGET http://master_server/list/server
