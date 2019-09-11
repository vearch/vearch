// Copyright 2018 The Couchbase Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package util

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

type GeoLocation struct {
	Lat float64 `json:"lat,omitempty"`
	Lon float64 `json:"lon,omitempty"`
}

const EARTH_RADIUS = 6378137
const EARTH_RADIUS_FLOAT64 = float64(EARTH_RADIUS)
const RAD = math.Pi / 180.0

// Earth semi long axis defined by WGS84 in meters
const EARTH_SEMI_LONG_AXIS = 6378137.0

// Earth semi short axis defined by WGS84 in meters
const EARTH_SEMI_SHORT_AXIS = 6378137.0
const EARTH_EQUATOR = 2 * math.Pi * EARTH_SEMI_LONG_AXIS

func GeoLocationFromOrigin(origin string) (GeoLocation, error) {
	geo := strings.Split(origin, ",")
	if len(geo) < 2 {
		return GeoLocation{}, fmt.Errorf("unparser geo origin: %s", origin)
	}

	latStr := strings.TrimSpace(geo[0])
	lonStr := strings.TrimSpace(geo[1])

	lat, err := strconv.ParseFloat(latStr, 64)
	if err != nil {
		return GeoLocation{}, err
	}
	lon, err := strconv.ParseFloat(lonStr, 64)
	if err != nil {
		return GeoLocation{}, err
	}

	geoLocation := GeoLocation{lat, lon}
	return geoLocation, nil
}

func GeoDistanceAre(lat1, lon1, lat2, lon2 float64) (float64, error) {
	Lat1 := rad(lat1)
	Lat2 := rad(lat2)
	a := Lat1 - Lat2
	b := rad(lon1) - rad(lon2)
	s := 2 * math.Asin(math.Sqrt(math.Pow(math.Sin(a/2), 2)+math.Cos(Lat1)*math.Cos(Lat2)*math.Pow(math.Sin(b/2), 2)))
	s = s * 6378137.0
	s = math.Round(s*10000) / 10000
	return float64(s), nil
}

func GeoDistancePlane(lat1, lon1, lat2, lon2 float64) (float64, error) {
	px := lon2 - lon1
	py := lat2 - lat1
	disntance := math.Sqrt(px*px+py*py) * DistancePerDegree()
	return disntance, nil
}

func DistancePerDegree() float64 {
	return EARTH_EQUATOR / 360.0
}

func rad(d float64) float64 {
	return d * math.Pi / 180.0
}
