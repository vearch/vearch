// Copyright 2015 The Cockroach Authors.
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

package export

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/vearch/vearch/util/cbbytes"
	"github.com/vearch/vearch/util/metrics"
)

var (
	escape                = strings.NewReplacer("\\", `\\`, "\n", `\n`)
	escapeWithDoubleQuote = strings.NewReplacer("\\", `\\`, "\n", `\n`, "\"", `\"`)
)

// WriteSample writes a single sample in text format to out.
func WriteSample(name string, m *metrics.MetricData, labelName, labelValue string, value float64, out io.Writer) error {
	if _, err := fmt.Fprint(out, name); err != nil {
		return err
	}
	if err := LabelPairsToText(m.Labels, labelName, labelValue, out); err != nil {
		return err
	}

	val := ""
	switch m.Unit {
	case metrics.Unit_BYTES:
		val = cbbytes.FormatIByte(uint64(value))
	case metrics.Unit_PERCENT:
		val = strconv.FormatFloat(value*100, 'f', 2, 64) + "%"
	case metrics.Unit_NANOSECONDS:
		val = strconv.FormatFloat(value, 'f', 0, 64) + "ns"
	case metrics.Unit_SECONDS:
		val = strconv.FormatFloat(value, 'f', 2, 64) + "s"
	default:
		val = strconv.FormatFloat(value, 'f', 2, 64)
	}
	if _, err := fmt.Fprintf(out, " value=%s", val); err != nil {
		return err
	}
	if m.TimestampNs != 0 {
		if _, err := fmt.Fprintf(out, " timeNS=%v", m.TimestampNs); err != nil {
			return err
		}
	}
	if _, err := out.Write([]byte{'\n'}); err != nil {
		return err
	}
	return nil
}

// LabelPairsToText converts a slice of LabelPair plus the explicitly given additional label pair into text format and writes it to 'out'.
// escaped as required by the text format, and enclosed in '{...}'.
func LabelPairsToText(labels []metrics.LabelPair, additionalName, additionalValue string, out io.Writer) error {
	if len(labels) == 0 && additionalName == "" {
		return nil
	}

	separator := '{'
	for _, label := range labels {
		if _, err := fmt.Fprintf(out, `%c%s="%s"`, separator, label.Name, EscapeString(label.Value, true)); err != nil {
			return err
		}
		separator = ','
	}

	if additionalName != "" {
		if _, err := fmt.Fprintf(out, `%c%s="%s"`, separator, additionalName, EscapeString(additionalValue, true)); err != nil {
			return err
		}
	}
	if _, err := out.Write([]byte{'}'}); err != nil {
		return err
	}
	return nil
}

func EscapeString(v string, includeDoubleQuote bool) string {
	if includeDoubleQuote {
		return escapeWithDoubleQuote.Replace(v)
	}

	return escape.Replace(v)
}
