package models

import (
	"fmt"
	"log"
	"math"
	"time"

	"github.com/influxdata/telegraf"
)

type RunningInput struct {
	Input  telegraf.Input
	Config *InputConfig

	trace       bool
	debug       bool
	defaultTags map[string]string
}

// InputConfig containing a name, interval, and filter
type InputConfig struct {
	Name              string
	NameOverride      string
	MeasurementPrefix string
	MeasurementSuffix string
	Tags              map[string]string
	Filter            Filter
	Interval          time.Duration
}

func (ri *RunningInput) Name() string {
	return "inputs." + ri.Config.Name
}

// MakeMetric either returns a metric, or returns nil if the metric doesn't
// need to be created (because of filtering, an error, etc.)
func (ri *RunningInput) MakeMetric(
	measurement string,
	fields map[string]interface{},
	tags map[string]string,
	mType telegraf.ValueType,
	t time.Time,
) telegraf.Metric {
	if len(fields) == 0 || len(measurement) == 0 {
		return nil
	}
	if tags == nil {
		tags = make(map[string]string)
	}

	// Override measurement name if set
	if len(ri.Config.NameOverride) != 0 {
		measurement = ri.Config.NameOverride
	}
	// Apply measurement prefix and suffix if set
	if len(ri.Config.MeasurementPrefix) != 0 {
		measurement = ri.Config.MeasurementPrefix + measurement
	}
	if len(ri.Config.MeasurementSuffix) != 0 {
		measurement = measurement + ri.Config.MeasurementSuffix
	}

	// Apply plugin-wide tags if set
	for k, v := range ri.Config.Tags {
		if _, ok := tags[k]; !ok {
			tags[k] = v
		}
	}
	// Apply daemon-wide tags if set
	for k, v := range ri.defaultTags {
		if _, ok := tags[k]; !ok {
			tags[k] = v
		}
	}

	// Apply the metric filter(s)
	if ok := ri.Config.Filter.Apply(measurement, fields, tags); !ok {
		return nil
	}

	for k, v := range fields {
		// Validate uint64 and float64 fields
		// convert all int & uint types to int64
		switch val := v.(type) {
		case uint:
			fields[k] = int64(val)
			continue
		case uint8:
			fields[k] = int64(val)
			continue
		case uint16:
			fields[k] = int64(val)
			continue
		case uint32:
			fields[k] = int64(val)
			continue
		case int:
			fields[k] = int64(val)
			continue
		case int8:
			fields[k] = int64(val)
			continue
		case int16:
			fields[k] = int64(val)
			continue
		case int32:
			fields[k] = int64(val)
			continue
		case uint64:
			// InfluxDB does not support writing uint64
			if val < uint64(9223372036854775808) {
				fields[k] = int64(val)
			} else {
				fields[k] = int64(9223372036854775807)
			}
			continue
		case float32:
			fields[k] = float64(val)
			continue
		case float64:
			// NaNs are invalid values in influxdb, skip measurement
			if math.IsNaN(val) || math.IsInf(val, 0) {
				if ri.debug {
					log.Printf("Measurement [%s] field [%s] has a NaN or Inf "+
						"field, skipping",
						measurement, k)
				}
				delete(fields, k)
				continue
			}
		}

		fields[k] = v
	}

	var m telegraf.Metric
	var err error
	switch mType {
	case telegraf.Counter:
		m, err = telegraf.NewCounterMetric(measurement, tags, fields, t)
	case telegraf.Gauge:
		m, err = telegraf.NewGaugeMetric(measurement, tags, fields, t)
	default:
		m, err = telegraf.NewMetric(measurement, tags, fields, t)
	}
	if err != nil {
		log.Printf("Error adding point [%s]: %s\n", measurement, err.Error())
		return nil
	}

	if ri.trace {
		fmt.Println("> " + m.String())
	}

	return m
}

func (ri *RunningInput) Debug() bool {
	return ri.debug
}

func (ri *RunningInput) SetDebug(debug bool) {
	ri.debug = debug
}

func (ri *RunningInput) Trace() bool {
	return ri.trace
}

func (ri *RunningInput) SetTrace(trace bool) {
	ri.trace = trace
}

func (ri *RunningInput) SetDefaultTags(tags map[string]string) {
	ri.defaultTags = tags
}
