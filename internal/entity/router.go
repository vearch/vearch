package entity

import "golang.org/x/time/rate"

type RouterLimitCfg struct {
	RequestLimitEnabled bool    `json:"request_limit_enabled"`
	TotalReadLimit      float64 `json:"read_request_limit_count,omitempty"`
	TotalWriteLimit     float64 `json:"write_request_limit_count,omitempty"`
}
type Router struct {
	Count       float64
	LimitConfig *RouterLimitCfg
}

const (
	DefaultReadRequestLimitCount  = 1000000.0
	DefaultWriteRequestLimitCount = 1000000.0
)

var (
	ReadLimiter  = rate.NewLimiter(rate.Limit(rate.Inf), 0)
	WriteLimiter = rate.NewLimiter(rate.Limit(rate.Inf), 0)
)

var routerInfo = &Router{
	Count:       0.0,
	LimitConfig: &RouterLimitCfg{},
}

func SetRequestLimit(router *RouterLimitCfg) {
	if router.RequestLimitEnabled {
		routerInfo.LimitConfig.RequestLimitEnabled = true

		if router.TotalReadLimit > 0 {
			routerInfo.LimitConfig.TotalReadLimit = router.TotalReadLimit
		} else {
			routerInfo.LimitConfig.TotalReadLimit = DefaultReadRequestLimitCount
		}

		if router.TotalWriteLimit > 0 {
			routerInfo.LimitConfig.TotalWriteLimit = router.TotalWriteLimit

		} else {
			routerInfo.LimitConfig.TotalWriteLimit = DefaultWriteRequestLimitCount
		}

		if routerInfo.Count > 0 {
			var limit rate.Limit
			limit = rate.Limit(routerInfo.LimitConfig.TotalReadLimit / routerInfo.Count)
			ReadLimiter.SetLimit(limit)
			ReadLimiter.SetBurst(int(limit * 1.1))

			limit = rate.Limit(routerInfo.LimitConfig.TotalWriteLimit / routerInfo.Count)
			WriteLimiter.SetLimit(limit)
			WriteLimiter.SetBurst(int(limit * 1.1))
		}
	} else {
		routerInfo.LimitConfig.RequestLimitEnabled = false
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

	if routerInfo.LimitConfig.RequestLimitEnabled && routerInfo.Count > 0 {
		limit := rate.Limit(routerInfo.LimitConfig.TotalReadLimit / routerInfo.Count)
		ReadLimiter.SetLimit(limit)
		ReadLimiter.SetBurst(int(limit * 1.1))

		limit = rate.Limit(routerInfo.LimitConfig.TotalWriteLimit / routerInfo.Count)
		WriteLimiter.SetLimit(limit)
		WriteLimiter.SetBurst(int(limit * 1.1))
	}
}
