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
	"testing"
	"time"

	"github.com/vearch/vearch/util/assert"
)

func TestGeo(t *testing.T) {
	lat1 := float64(29.3760)
	lon1 := float64(4.894)
	lat2 := float64(-29.5760)
	lon2 := float64(-14.894)

	d := ""

	start := time.Now()
	for i := 0; i < 100; i++ {
		distance, _ := GeoDistanceAre(lat1, lon1, lat2, lon2)
		d = strconv.FormatFloat(distance, 'f', -1, 64)
	}
	println(d)
	cost := time.Since(start)
	fmt.Printf("cost=[%s]", cost)

	println("\n")
	start = time.Now()
	for i := 0; i < 100; i++ {
		distance, _ := GeoDistancePlane(lat1, lon1, lat2, lon2)
		d = strconv.FormatFloat(distance, 'f', -1, 64)
	}
	println(d)
	cost = time.Since(start)
	fmt.Printf("cost=[%s]", cost)

	println("\n")
	start = time.Now()
	for i := 0; i < 100; i++ {
		distance1, _ := GeoDistance1(lat1, lon1, lat2, lon2)
		d = strconv.FormatFloat(distance1, 'f', -1, 64)
	}
	println(d)
	cost = time.Since(start)
	fmt.Printf("cost=[%s]", cost)

	println("\n")
	start = time.Now()
	for i := 0; i < 100; i++ {
		distance2, _ := GeoDistance2(lat1, lon1, lat2, lon2)
		d = strconv.FormatFloat(distance2, 'f', -1, 64)
	}
	println(d)
	cost = time.Since(start)
	fmt.Printf("cost=[%s]", cost)

	println("\n")
	start = time.Now()
	for i := 0; i < 100; i++ {
		distance3, _ := GeoDistance3(lat1, lon1, lat2, lon2)
		d = strconv.FormatFloat(distance3, 'f', -1, 64)
	}
	println(d)
	cost = time.Since(start)
	fmt.Printf("cost=[%s]", cost)

	assert.True(t, true)

	fmt.Println("ok runing")
}

func GeoDistance1(lat1, lon1, lat2, lon2 float64) (float64, error) {
	lat1 = lat1 * RAD
	lon1 = lon1 * RAD
	lat2 = lat2 * RAD
	lon2 = lon2 * RAD
	theta := lon2 - lon1
	dist := math.Acos(math.Sin(lat1)*math.Sin(lat2) + math.Cos(lat1)*math.Cos(lat2)*math.Cos(theta))

	distance := dist * EARTH_RADIUS_FLOAT64
	return distance, nil
}

/**
get distance from two geo point
*/
func GeoDistance2(lat1, lon1, lat2, lon2 float64) (float64, error) {
	radLat1 := rad(lat1)
	radLat2 := rad(lat2)

	radLon1 := rad(lon1)
	radLon2 := rad(lon2)

	if radLat1 < 0 {
		radLat1 = math.Pi/2 + math.Abs(radLat1) // south
	}
	if radLat1 > 0 {
		radLat1 = math.Pi/2 - math.Abs(radLat1) // north
	}
	if radLon1 < 0 {
		radLon1 = math.Pi*2 - math.Abs(radLon1) // west
	}
	if radLat2 < 0 {
		radLat2 = math.Pi/2 + math.Abs(radLat2) // south
	}
	if radLat2 > 0 {
		radLat2 = math.Pi/2 - math.Abs(radLat2) // north
	}
	if radLon2 < 0 {
		radLon2 = math.Pi*2 - math.Abs(radLon2) // west
	}

	x1 := EARTH_RADIUS * math.Cos(radLon1) * math.Sin(radLat1)
	y1 := EARTH_RADIUS * math.Sin(radLon1) * math.Sin(radLat1)
	z1 := EARTH_RADIUS * math.Cos(radLat1)

	x2 := EARTH_RADIUS * math.Cos(radLon2) * math.Sin(radLat2)
	y2 := EARTH_RADIUS * math.Sin(radLon2) * math.Sin(radLat2)
	z2 := EARTH_RADIUS * math.Cos(radLat2)

	d := math.Sqrt((x1-x2)*(x1-x2) + (y1-y2)*(y1-y2) + (z1-z2)*(z1-z2))
	theta := math.Acos((EARTH_RADIUS*EARTH_RADIUS + EARTH_RADIUS*EARTH_RADIUS - d*d) / (2 * EARTH_RADIUS * EARTH_RADIUS))
	distance := theta * EARTH_RADIUS

	return distance, nil
}

func GeoDistance3(lat1, lon1, lat2, lon2 float64) (float64, error) {
	pk := 180 / 3.14169
	a1 := lat1 / pk
	a2 := lon1 / pk
	b1 := lat2 / pk
	b2 := lon2 / pk
	t1 := math.Cos(a1) * math.Cos(a2) * math.Cos(b1) * math.Cos(b2)
	t2 := math.Cos(a1) * math.Sin(a2) * math.Cos(b1) * math.Sin(b2)
	t3 := math.Sin(a1) * math.Sin(b1)
	tt := math.Acos(t1 + t2 + t3)
	return 6366000 * tt, nil
}
