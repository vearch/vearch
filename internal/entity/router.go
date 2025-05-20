package entity

import "golang.org/x/time/rate"

type RouterLimitCfg struct {
	RequestLimit    bool    `json:"request_limit_enabled,omitempty"`
	TotalReadLimit  float64 `json:"read_request_limit_count,omitempty"`
	TotalWriteLimit float64 `json:"write_request_limit_count,omitempty"`
}
type Router struct {
	Count       float64
	LimitConfig *RouterLimitCfg
}

const (
	DefaultReadRequestLimitCount  = 1000000.0
	DefaultWriteRequestLimitCount = 1000000.0
)

var RouterCount float64 = 0.0

var (
	ReadLimiter  = rate.NewLimiter(rate.Limit(rate.Inf), 0)
	WriteLimiter = rate.NewLimiter(rate.Limit(rate.Inf), 0)
)

var routerInfo = &Router{
	Count:       0.0,
	LimitConfig: &RouterLimitCfg{},
}

func SetRequestLimit(router *RouterLimitCfg) {
	if router.RequestLimit && routerInfo.Count > 0 {
		var limit rate.Limit

		routerInfo.LimitConfig.RequestLimit = true
		if router.TotalReadLimit > 0 {
			routerInfo.LimitConfig.TotalReadLimit = router.TotalReadLimit
		} else {
			routerInfo.LimitConfig.TotalReadLimit = DefaultReadRequestLimitCount
		}

		limit = rate.Limit(routerInfo.LimitConfig.TotalReadLimit / routerInfo.Count)
		ReadLimiter.SetLimit(limit)
		ReadLimiter.SetBurst(int(limit * 1.1))

		if router.TotalWriteLimit > 0 {
			routerInfo.LimitConfig.TotalWriteLimit = router.TotalWriteLimit

		} else {
			routerInfo.LimitConfig.TotalWriteLimit = DefaultWriteRequestLimitCount
		}

		limit = rate.Limit(router.TotalWriteLimit / RouterCount)
		WriteLimiter.SetLimit(limit)
		WriteLimiter.SetBurst(int(limit * 1.1))
	} else if !router.RequestLimit {
		routerInfo.LimitConfig.RequestLimit = false
		ReadLimiter.SetLimit(rate.Inf)
		ReadLimiter.SetBurst(0)

		WriteLimiter.SetLimit(rate.Inf)
		WriteLimiter.SetBurst(0)
	}
}

func SetRouterCount(add bool) {
	if add {
		routerInfo.Count++
	} else {
		routerInfo.Count--
	}

	if routerInfo.LimitConfig.RequestLimit && routerInfo.Count > 0 {
		limit := rate.Limit(routerInfo.LimitConfig.TotalReadLimit / routerInfo.Count)
		ReadLimiter.SetLimit(limit)
		ReadLimiter.SetBurst(int(limit * 1.1))

		limit = rate.Limit(routerInfo.LimitConfig.TotalWriteLimit / routerInfo.Count)
		WriteLimiter.SetLimit(limit)
		WriteLimiter.SetBurst(int(limit * 1.1))
	}
}
