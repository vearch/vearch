package entity

import "golang.org/x/time/rate"

type Router struct {
	Query_limit  bool
	Router_count float32
}

const query_limit_total = 100000.0

var RWLimiter = rate.NewLimiter(rate.Limit(rate.Inf), 0)
var routerInfo = &Router{}

func SetQueryLimit(query_limit bool) {
	routerInfo.Query_limit = query_limit

	if query_limit && routerInfo.Router_count > 0 {
		limit := rate.Limit(query_limit_total / routerInfo.Router_count)
		RWLimiter.SetLimit(limit)
		RWLimiter.SetBurst(int(limit + 1000))
	} else if !query_limit {
		RWLimiter.SetLimit(rate.Inf)
		RWLimiter.SetBurst(0)
	}
}

func SetRouterCount(add bool) {
	if add {
		routerInfo.Router_count++
	} else {
		routerInfo.Router_count--
	}
	if routerInfo.Query_limit && routerInfo.Router_count > 0 {
		limit := rate.Limit(query_limit_total / routerInfo.Router_count)
		RWLimiter.SetLimit(limit)
		RWLimiter.SetBurst(int(limit + 1000))
	}
}
