package monitor

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/caio/go-tdigest"
)

func TestTdigest(t *testing.T) {
	td, _ := tdigest.New(tdigest.Compression(100))
	for i := 0; i < 10000; i++ {
		td.Add(rand.Float64())
	}

	tp50 := td.Quantile(0.5)

	fmt.Printf("count:%v\n", td.Count())
	fmt.Printf("p(.5) = %.6f\n", tp50)
	fmt.Printf("CDF(Quantile(.5)) = %.6f\n", td.CDF(td.Quantile(0.5)))

	_startTime := time.Now().Nanosecond()
	fmt.Println(_startTime)

	for i := 0; i < 10000; i++ {
		Profiler("bulk_insert_"+string(rand.Intn(10)+65), time.Now().Nanosecond())
	}

	my := SliceMetric()
	for _, element := range my {
		fmt.Printf("key:%v----Sum=%v---Count=%v-- max=%v--\n", element.Name, element.Sum, element.Digest.Count(), element.Digest.Quantile(1))
	}

	Profiler("bulk_insert", time.Now().Nanosecond())

	defer Profiler("defer", time.Now().Nanosecond())

	fmt.Printf("time Sleep = %v  \n", time.Now().Nanosecond())
	time.Sleep(time.Duration(2) * time.Second)
	fmt.Printf("time Sleep after = %v \n", time.Now().Nanosecond())

}
