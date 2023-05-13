module github.com/vearch/vearch

go 1.15

replace (
	golang.org/x/time => golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	google.golang.org/grpc => google.golang.org/grpc v1.2.1-0.20180928173848-b48e364c83c8
)

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/StackExchange/wmi v1.2.1 // indirect
	github.com/apache/thrift v0.13.0 // indirect
	github.com/caio/go-tdigest v3.1.0+incompatible
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/codahale/hdrhistogram v0.9.0
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/dgrijalva/jwt-go v3.2.1-0.20190620180102-5e25c22bd5d6+incompatible // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/fatih/color v1.7.1-0.20181010231311-3f9d52f7176a // indirect
	github.com/fsnotify/fsnotify v1.4.9 // indirect
	github.com/ghodss/yaml v1.0.1-0.20190212211648-25d852aebe32 // indirect
	github.com/gin-gonic/gin v1.7.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/mock v1.5.0 // indirect
	github.com/golang/protobuf v1.5.2
	github.com/google/btree v1.0.1 // indirect
	github.com/google/flatbuffers v1.11.1-0.20191218192354-ce3a1c43a288
	github.com/google/uuid v1.3.0 // indirect
	github.com/gorilla/mux v1.6.3-0.20180903154305-9e1f5955c0d2
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/hashicorp/consul/api v1.1.0 // indirect
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/json-iterator/go v1.1.12
	github.com/juju/ratelimit v1.0.1
	github.com/julienschmidt/httprouter v1.3.0
	github.com/kavu/go_reuseport v1.4.1-0.20181221084137-1f6171f327ed // indirect
	github.com/leesper/go_rng v0.0.0-20190531154944-a612b043e353 // indirect
	github.com/miekg/dns v1.1.25 // indirect
	github.com/mitchellh/mapstructure v1.4.1 // indirect
	github.com/mmcloughlin/geohash v0.0.0-20181009053802-f7f2bcae3294
	github.com/opentracing/opentracing-go v1.1.0
	github.com/patrickmn/go-cache v2.1.1-0.20180815053127-5633e0862627+incompatible
	github.com/pkg/errors v0.9.1
	github.com/pkg/sftp v1.10.1
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/rs/cors v1.6.1-0.20190613161432-33ffc0734c60 // indirect
	github.com/shirou/gopsutil v2.17.13-0.20180927124308-a11c78ba2c13+incompatible
	github.com/shirou/w32 v0.0.0-20160930032740-bb4de0191aa4 // indirect
	github.com/shopspring/decimal v1.3.1
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/smallnest/pool v0.0.0-20170926025334-4f76a6d6402e
	github.com/smallnest/rpcx v1.4.2-0.20190627094758-28d08d166104
	github.com/soheilhy/cmux v0.1.5 // indirect
	github.com/spaolacci/murmur3 v1.1.0
	github.com/spf13/cast v1.3.1
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tiglabs/raft v0.0.0-20200304095606-b25a44ad8b33
	github.com/tmc/grpc-websocket-proxy v0.0.0-20201229170055-e5319fda7802 // indirect
	github.com/uber/jaeger-client-go v2.30.0+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/valyala/fastjson v1.1.1
	github.com/vmihailenco/msgpack v4.0.4+incompatible
	go.etcd.io/bbolt v1.3.6 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20190801225801-f1c7fd3d53b0
	go.opencensus.io v0.23.0 // indirect
	go.uber.org/atomic v1.9.0
	go.uber.org/multierr v1.7.0 // indirect
	go.uber.org/zap v1.19.1 // indirect
	golang.org/x/crypto v0.0.0-20210921155107-089bfa567519
	golang.org/x/exp v0.0.0-20200224162631-6cc2880d07d6
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/net v0.0.0-20211101193420-4a448f8816b3
	golang.org/x/sys v0.0.0-20211101204403-39c9dd37992c // indirect
	golang.org/x/text v0.3.7 // indirect
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac // indirect
	gonum.org/v1/gonum v0.9.3 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20211101144312-62acf1d99145 // indirect
	google.golang.org/grpc v1.41.0
	gotest.tools v2.1.1-0.20181001141646-317cc193f525+incompatible
	sigs.k8s.io/yaml v1.3.0 // indirect
)
