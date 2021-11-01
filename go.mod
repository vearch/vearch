module github.com/vearch/vearch

go 1.15

exclude github.com/vearch/vearch/engine/third_party/flatbuffers-1.11.0 v1.11.0

replace (
	github.com/gogo/protobuf => github.com/gogo/protobuf v1.2.1-0.20181231100452-8e4a75f11384
	github.com/golang/protobuf => github.com/golang/protobuf v1.2.1-0.20190109072247-347cf4a86c1c
	github.com/gorilla/mux => github.com/gorilla/mux v1.6.3-0.20180903154305-9e1f5955c0d2
	github.com/json-iterator/go => github.com/json-iterator/go v1.1.6-0.20180914014843-2433035e5132
	github.com/julienschmidt/httprouter => github.com/julienschmidt/httprouter v1.2.1-0.20181021223831-26a05976f9bf
	github.com/mmcloughlin/geohash => github.com/mmcloughlin/geohash v0.0.0-20181009053802-f7f2bcae3294
	github.com/patrickmn/go-cache => github.com/patrickmn/go-cache v2.1.1-0.20180815053127-5633e0862627+incompatible
	github.com/prometheus/client_golang => github.com/prometheus/client_golang v0.9.2-0.20181121042956-32b1bb4674c4
	github.com/shirou/gopsutil => github.com/shirou/gopsutil v2.17.13-0.20180927124308-a11c78ba2c13+incompatible
	github.com/smallnest/pool => github.com/smallnest/pool v0.0.0-20170926025334-4f76a6d6402e
	github.com/smallnest/rpcx => github.com/smallnest/rpcx v1.4.2-0.20190627094758-28d08d166104
	github.com/soheilhy/cmux => github.com/soheilhy/cmux v0.1.5
	github.com/spaolacci/murmur3 => github.com/spaolacci/murmur3 v1.1.0
	github.com/spf13/cast => github.com/spf13/cast v1.3.0
	github.com/tiglabs/raft => github.com/tiglabs/raft v0.0.0-20190131082128-45667fcdb8b8
	github.com/valyala/fastjson => github.com/valyala/fastjson v1.1.1
	github.com/vmihailenco/msgpack => github.com/vmihailenco/msgpack v4.0.1+incompatible
	golang.org/x/time => golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	google.golang.org/grpc => google.golang.org/grpc v1.2.1-0.20180928173848-b48e364c83c8
)

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/StackExchange/wmi v1.2.1 // indirect
	github.com/caio/go-tdigest v3.1.0+incompatible
	github.com/codahale/hdrhistogram v0.9.0
	github.com/dustin/gojson v0.0.0-20160307161227-2e71ec9dd5ad
	github.com/gin-gonic/gin v1.3.1-0.20190109013244-29a145c85dc0
	github.com/gogo/protobuf v1.2.1
	github.com/golang/protobuf v1.3.2
	github.com/google/flatbuffers v1.11.1-0.20191218192354-ce3a1c43a288
	github.com/gorilla/mux v1.7.3
	github.com/jasonlvhit/gocron v0.0.0-20190121134850-6771d4b492ba
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
	github.com/shirou/w32 v0.0.0-20160930032740-bb4de0191aa4 // indirect
	github.com/shopspring/decimal v1.2.0
	github.com/smallnest/pool v0.0.0-20170926025334-4f76a6d6402e
	github.com/smallnest/rpcx v1.6.11
	github.com/spaolacci/murmur3 v1.1.0
	github.com/spf13/cast v1.4.1
	github.com/tiglabs/raft v0.0.0-20200304095606-b25a44ad8b33
	github.com/valyala/fastjson v1.6.3
	github.com/vmihailenco/msgpack v4.0.4+incompatible
	go.etcd.io/etcd v0.5.0-alpha.5.0.20190801225801-f1c7fd3d53b0
	go.uber.org/atomic v1.9.0
	golang.org/x/crypto v0.0.0-20210817164053-32db794688a5
	golang.org/x/exp v0.0.0-20210903233438-a2d0902c3ac7
	golang.org/x/net v0.0.0-20210903162142-ad29c8ab022f
	golang.org/x/sys v0.0.0-20210816074244-15123e1e1f71 // indirect
	gonum.org/v1/gonum v0.9.3 // indirect
	google.golang.org/grpc v1.26.0
	gotest.tools v2.2.0+incompatible
	sigs.k8s.io/yaml v1.3.0 // indirect
)
