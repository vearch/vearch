package monitoring

import (
	"net/http"
	"time"

	prometheus "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cast"
	"github.com/vearch/vearch/config"
	"github.com/vearch/vearch/util/log"
)

var masterCallBack func(masterMonitor *MasterMonitor)

type MasterMonitor struct {
	Cpu      *prometheus.GaugeVec
	Mem      *prometheus.GaugeVec
	Fs       *prometheus.GaugeVec
	NetIn    *prometheus.GaugeVec
	NetOut   *prometheus.GaugeVec
	Gc       *prometheus.GaugeVec
	Routines *prometheus.GaugeVec

	//all has
	PartitionNum *prometheus.GaugeVec

	//schema num
	ServerNum prometheus.Gauge
	DBNum     prometheus.Gauge
	SpaceNum  prometheus.Gauge
	SpaceDoc  *prometheus.GaugeVec
	SpaceSize *prometheus.GaugeVec

	//ps value
	PSLeaderNum     *prometheus.GaugeVec
	PSPartitionSize *prometheus.GaugeVec
	PSPartitionDoc  *prometheus.GaugeVec
}

func RegisterMaster(call func(masterMonitor *MasterMonitor)) {

	masterCallBack = call

	mm := &MasterMonitor{
		Cpu:          newGaugeVec("cpu", "cpu", "type", "ip"),
		Mem:          newGaugeVec("mem", "memory", "type", "ip"),
		Fs:           newGaugeVec("fs", "file disk", "type", "ip"),
		NetIn:        newGaugeVec("net_in", "net in per second", "type", "ip"),
		NetOut:       newGaugeVec("net_out", "net out per second", "type", "ip"),
		Gc:           newGaugeVec("gc", "go gc", "type", "ip"),
		Routines:     newGaugeVec("routines", "go routines", "type", "ip"),
		ServerNum:    newGauge("serverNum", "server number"),
		DBNum:        newGauge("db_num", "database number"),
		SpaceNum:     newGauge("space_num", "space number"),
		SpaceDoc:     newGaugeVec("space_doc", "space document number", "db_name", "space_name", "space_id"),
		SpaceSize:    newGaugeVec("space_size", "space disk size", "db_name", "space_name", "space_id"),
		PartitionNum: newGaugeVec("partition_num", "space document number", "type", "ip"),

		PSLeaderNum:     newGaugeVec("leader_num", "partition has number", "ip"),
		PSPartitionSize: newGaugeVec("partition_size", "single partition size", "ip", "partition_id"),
		PSPartitionDoc:  newGaugeVec("partition_doc", "single partition doc number", "ip", "partition_id"),
	}

	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(mm.Cpu, mm.Mem, mm.Fs, mm.NetIn, mm.NetOut, mm.Gc, mm.Routines, mm.ServerNum, mm.DBNum, mm.SpaceNum, mm.SpaceDoc, mm.SpaceSize, mm.PartitionNum)
	registry.MustRegister(mm.PSLeaderNum, mm.PSPartitionSize, mm.PSPartitionDoc)

	go func() {
		defer func() {
			if e := recover(); e != nil {
				log.Error("monitor has err:[%v]", e)
			}
		}()
		for {
			time.Sleep(time.Second * 15)
			if masterCallBack != nil {
				masterCallBack(mm)
			}
		}
	}()

	go func() {
		self := config.Conf().Masters.Self()
		http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
		if err := http.ListenAndServe(":"+cast.ToString(self.MonitorPort), nil); err != nil {
			panic(err)
		}
	}()
}

func newCount(name, help string) *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: name,
			Help: help,
		},
		nil,
	)
}

func newGaugeVec(Name, help string, labels ...string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: Name,
		Help: help,
	}, labels)

}

func newGauge(Name, help string) prometheus.Gauge {
	return prometheus.NewGauge(prometheus.GaugeOpts{
		Name: Name,
		Help: help,
	})

}

func ToContent() {
	promhttp.Handler()
}
