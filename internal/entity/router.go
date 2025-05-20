package entity

import "golang.org/x/time/rate"

type Router struct {
	RequestLimit bool
	Count        float32
}

const request_limit_total = 100000.0

var RWLimiter = rate.NewLimiter(rate.Limit(rate.Inf), 0)
var routerInfo = &Router{}

func SetRequestLimit(request_limit bool) {
	routerInfo.RequestLimit = request_limit

	if request_limit && routerInfo.Count > 0 {
		limit := rate.Limit(request_limit_total / routerInfo.Count)
		RWLimiter.SetLimit(limit)
		RWLimiter.SetBurst(int(limit + 1000))
	} else if !request_limit {
		RWLimiter.SetLimit(rate.Inf)
		RWLimiter.SetBurst(0)
	}
}

func SetRouterCount(add bool) {
	if add {
		routerInfo.Count++
	} else {
		routerInfo.Count--
	}
	if routerInfo.RequestLimit && routerInfo.Count > 0 {
		limit := rate.Limit(request_limit_total / routerInfo.Count)
		RWLimiter.SetLimit(limit)
		RWLimiter.SetBurst(int(limit + 1000))
	}
}
