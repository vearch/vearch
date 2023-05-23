module github.com/vearch/vearch

go 1.19

replace (
	golang.org/x/time => golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	google.golang.org/grpc => google.golang.org/grpc v1.2.1-0.20180928173848-b48e364c83c8
)

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/caio/go-tdigest v3.1.0+incompatible
	github.com/codahale/hdrhistogram v0.9.0
	github.com/gin-gonic/gin v1.7.0
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.2
	github.com/google/flatbuffers v1.11.1-0.20191218192354-ce3a1c43a288
	github.com/gorilla/mux v1.6.3-0.20180903154305-9e1f5955c0d2
	github.com/json-iterator/go v1.1.12
	github.com/juju/ratelimit v1.0.1
	github.com/julienschmidt/httprouter v1.3.0
	github.com/mmcloughlin/geohash v0.0.0-20181009053802-f7f2bcae3294
	github.com/opentracing/opentracing-go v1.1.0
	github.com/patrickmn/go-cache v2.1.1-0.20180815053127-5633e0862627+incompatible
	github.com/pkg/errors v0.9.1
	github.com/pkg/sftp v1.10.1
	github.com/prometheus/client_golang v1.11.1
	github.com/shirou/gopsutil v2.17.13-0.20180927124308-a11c78ba2c13+incompatible
	github.com/shopspring/decimal v1.3.1
	github.com/smallnest/pool v0.0.0-20170926025334-4f76a6d6402e
	github.com/smallnest/rpcx v1.4.2-0.20190627094758-28d08d166104
	github.com/spaolacci/murmur3 v1.1.0
	github.com/spf13/cast v1.3.1
	github.com/tiglabs/raft v0.0.0-20200304095606-b25a44ad8b33
	github.com/uber/jaeger-client-go v2.30.0+incompatible
	github.com/valyala/fastjson v1.1.1
	github.com/vmihailenco/msgpack v4.0.4+incompatible
	go.etcd.io/etcd v0.5.0-alpha.5.0.20190801225801-f1c7fd3d53b0
	go.uber.org/atomic v1.9.0
	golang.org/x/crypto v0.1.0
	golang.org/x/exp v0.0.0-20200224162631-6cc2880d07d6
	golang.org/x/net v0.7.0
	google.golang.org/grpc v1.41.0
	gotest.tools v2.1.1-0.20181001141646-317cc193f525+incompatible
)

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.2 // indirect
	github.com/StackExchange/wmi v1.2.1 // indirect
	github.com/anacrolix/missinggo v1.1.0 // indirect
	github.com/anacrolix/sync v0.0.0-20180808010631-44578de4e778 // indirect
	github.com/anacrolix/utp v0.0.0-20180219060659-9e0e1d1d0572 // indirect
	github.com/apache/thrift v0.13.0 // indirect
	github.com/armon/go-metrics v0.0.0-20180917152333-f0300d1749da // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenk/backoff v2.1.1+incompatible // indirect
	github.com/cenkalti/backoff v2.1.1+incompatible // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/cheekybits/genny v1.0.0 // indirect
	github.com/coreos/etcd v3.3.13+incompatible // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20191104093116-d3cd4ed1dbcf // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/dgrijalva/jwt-go v3.2.1-0.20190620180102-5e25c22bd5d6+incompatible // indirect
	github.com/dgryski/go-jump v0.0.0-20170409065014-e1f439676b57 // indirect
	github.com/docker/libkv v0.2.1 // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/edwingeng/doublejump v0.0.0-20190102103700-461a0155c7be // indirect
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a // indirect
	github.com/fatih/color v1.7.1-0.20181010231311-3f9d52f7176a // indirect
	github.com/ghodss/yaml v1.0.1-0.20190212211648-25d852aebe32 // indirect
	github.com/gin-contrib/sse v0.1.0 // indirect
	github.com/go-ole/go-ole v1.2.5 // indirect
	github.com/go-playground/locales v0.13.0 // indirect
	github.com/go-playground/universal-translator v0.17.0 // indirect
	github.com/go-playground/validator/v10 v10.4.1 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/mock v1.5.0 // indirect
	github.com/google/btree v1.0.1 // indirect
	github.com/google/go-cmp v0.5.5 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gorilla/websocket v1.4.2 // indirect
	github.com/grandcat/zeroconf v0.0.0-20190424104450-85eadb44205c // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/hashicorp/consul/api v1.1.0 // indirect
	github.com/hashicorp/errwrap v1.0.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.1 // indirect
	github.com/hashicorp/go-immutable-radix v1.0.0 // indirect
	github.com/hashicorp/go-multierror v1.0.0 // indirect
	github.com/hashicorp/go-rootcerts v1.0.0 // indirect
	github.com/hashicorp/golang-lru v0.5.1 // indirect
	github.com/hashicorp/serf v0.8.2 // indirect
	github.com/huandu/xstrings v1.0.0 // indirect
	github.com/influxdata/influxdb1-client v0.0.0-20190402204710-8ff2fc3824fc // indirect
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/kavu/go_reuseport v1.4.1-0.20181221084137-1f6171f327ed // indirect
	github.com/klauspost/cpuid v1.2.1 // indirect
	github.com/klauspost/reedsolomon v1.9.1 // indirect
	github.com/kr/fs v0.1.0 // indirect
	github.com/leesper/go_rng v0.0.0-20190531154944-a612b043e353 // indirect
	github.com/leodido/go-urn v1.2.0 // indirect
	github.com/lucas-clemente/quic-go v0.11.0 // indirect
	github.com/marten-seemann/qtls v0.2.3 // indirect
	github.com/marten-seemann/quic-conn v0.0.0-20190404134349-539f7de6a079 // indirect
	github.com/mattn/go-colorable v0.1.1 // indirect
	github.com/mattn/go-isatty v0.0.12 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/miekg/dns v1.1.25 // indirect
	github.com/mitchellh/go-homedir v1.0.0 // indirect
	github.com/mitchellh/mapstructure v1.4.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20181016184325-3113b8401b8a // indirect
	github.com/rs/cors v1.6.1-0.20190613161432-33ffc0734c60 // indirect
	github.com/rubyist/circuitbreaker v2.2.1+incompatible // indirect
	github.com/samuel/go-zookeeper v0.0.0-20180130194729-c4fab1ac1bec // indirect
	github.com/shirou/w32 v0.0.0-20160930032740-bb4de0191aa4 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/soheilhy/cmux v0.1.5 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/tatsushid/go-fastping v0.0.0-20160109021039-d7bb493dee3e // indirect
	github.com/templexxx/cpufeat v0.0.0-20180724012125-cef66df7f161 // indirect
	github.com/templexxx/xor v0.0.0-20181023030647-4e92f724b73b // indirect
	github.com/tjfoc/gmsm v1.0.1 // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20201229170055-e5319fda7802 // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/ugorji/go/codec v1.1.7 // indirect
	github.com/valyala/fastrand v1.0.0 // indirect
	github.com/xiang90/probing v0.0.0-20190116061207-43a291ad63a2 // indirect
	github.com/xtaci/kcp-go v5.2.8+incompatible // indirect
	go.etcd.io/bbolt v1.3.6 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.uber.org/multierr v1.7.0 // indirect
	go.uber.org/zap v1.19.1 // indirect
	golang.org/x/sys v0.5.0 // indirect
	golang.org/x/text v0.7.0 // indirect
	golang.org/x/time v0.0.0-20210723032227-1f47c861a9ac // indirect
	gonum.org/v1/gonum v0.9.3 // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20211101144312-62acf1d99145 // indirect
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)
