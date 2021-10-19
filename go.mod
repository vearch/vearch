module github.com/vearch/vearch

go 1.16

replace github.com/codahale/hdrhistogram => github.com/HdrHistogram/hdrhistogram-go v0.9.0

replace github.com/coreos/bbolt => go.etcd.io/bbolt v1.3.4

replace github.com/smallnest/rpcx v1.6.11 => github.com/smallnest/rpcx v1.4.1

replace google.golang.org/grpc v1.36.0 => google.golang.org/grpc v1.26.0

replace github.com/soheilhy/cmux => github.com/soheilhy/cmux v0.1.5

replace go.etcd.io/etcd => go.etcd.io/etcd v0.0.0-20200520232829-54ba9589114f

exclude github.com/vearch/vearch/engine/third_party/flatbuffers-1.11.0 v1.11.0

require (
	github.com/BurntSushi/toml v0.4.1
	github.com/StackExchange/wmi v1.2.1 // indirect
	github.com/caio/go-tdigest v3.1.0+incompatible
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd
	github.com/coreos/bbolt v0.0.0-00010101000000-000000000000 // indirect
	github.com/coreos/etcd v3.3.25+incompatible // indirect
	github.com/dustin/gojson v0.0.0-20160307161227-2e71ec9dd5ad
	github.com/gin-gonic/gin v1.7.4
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.2
	github.com/google/flatbuffers v2.0.0+incompatible
	github.com/gorilla/mux v1.8.0
	github.com/jasonlvhit/gocron v0.0.1
	github.com/json-iterator/go v1.1.11
	github.com/juju/ratelimit v1.0.1
	github.com/julienschmidt/httprouter v1.3.0
	github.com/leesper/go_rng v0.0.0-20190531154944-a612b043e353 // indirect
	github.com/mmcloughlin/geohash v0.10.0
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/pkg/errors v0.9.1
	github.com/pkg/sftp v1.13.3
	github.com/prometheus/client_golang v1.11.0
	github.com/shirou/gopsutil v3.21.8+incompatible
	github.com/shopspring/decimal v1.2.0
	github.com/smallnest/pool v0.0.0-20170926025334-4f76a6d6402e
	github.com/smallnest/rpcx v1.6.11
	github.com/spaolacci/murmur3 v1.1.0
	github.com/spf13/cast v1.4.1
	github.com/tiglabs/raft v0.0.0-20200304095606-b25a44ad8b33
	github.com/tklauser/go-sysconf v0.3.9 // indirect
	github.com/valyala/fastjson v1.6.3
	github.com/vmihailenco/msgpack v4.0.4+incompatible
	go.etcd.io/etcd v0.0.0-20191023171146-3cf2f69b5738
	go.uber.org/atomic v1.9.0
	golang.org/x/crypto v0.0.0-20210817164053-32db794688a5
	golang.org/x/exp v0.0.0-20210903233438-a2d0902c3ac7
	golang.org/x/net v0.0.0-20210903162142-ad29c8ab022f
	gonum.org/v1/gonum v0.9.3 // indirect
	google.golang.org/grpc v1.36.0
	gotest.tools v2.2.0+incompatible
)
