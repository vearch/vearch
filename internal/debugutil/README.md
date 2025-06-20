# Vearch CPU/Memory performance analyze

## master/router node

**ipaddr**: remote ip address of master/router server
**pprof_port**: pprof_port config of master/router server

show web ui options:
http://ipaddr:pprof_port/debug/pprof/ui/

show profile result in web:

### CPU:

http://ipaddr:pprof_port/debug/pprof/ui/profile
http://ipaddr:pprof_port/debug/pprof/ui/profile?seconds=30

### Memory:

http://ipaddr:pprof_port/debug/pprof/ui/heap

## ps node

install perf tools in linux system
**ipaddr**: remote ip address of ps server
**pprof_port**: pprof_port config of ps server

### CPU:

http://ipaddr:pprof_port/perf/profile

### Memory:

http://ipaddr:pprof_port/perf/heap