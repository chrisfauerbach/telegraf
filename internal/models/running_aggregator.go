package models

import (
	"log"
	"time"

	"github.com/influxdata/telegraf"
)

type RunningAggregator struct {
	Aggregator telegraf.Aggregator
	Config     *AggregatorConfig
}

// AggregatorConfig containing configuration parameters for the running
// aggregator plugin.
type AggregatorConfig struct {
	Name string

	DropOriginal      bool
	NameOverride      string
	MeasurementPrefix string
	MeasurementSuffix string
	Tags              map[string]string
	Filter            Filter
}

func (ra *RunningAggregator) Name() string {
	return "aggregators." + ra.Config.Name
}

func (ra *RunningAggregator) MakeMetric(
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
	if len(ra.Config.NameOverride) != 0 {
		measurement = ra.Config.NameOverride
	}
	// Apply measurement prefix and suffix if set
	if len(ra.Config.MeasurementPrefix) != 0 {
		measurement = ra.Config.MeasurementPrefix + measurement
	}
	if len(ra.Config.MeasurementSuffix) != 0 {
		measurement = measurement + ra.Config.MeasurementSuffix
	}

	// Apply plugin-wide tags if set
	for k, v := range ra.Config.Tags {
		if _, ok := tags[k]; !ok {
			tags[k] = v
		}
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

	m.SetAggregate(true)

	return m
}

// Apply applies the given metric to the aggregator.
// Before applying to the plugin, it will run any defined filters on the metric.
// Apply returns true if the original metric should be dropped.
func (ra *RunningAggregator) Apply(in telegraf.Metric) bool {
	if ra.Config.Filter.IsActive() {
		// check if the aggregator should apply this metric
		name := in.Name()
		fields := in.Fields()
		tags := in.Tags()
		t := in.Time()
		if ok := ra.Config.Filter.Apply(name, fields, tags); !ok {
			// aggregator should not apply this metric
			return false
		}

		in, _ = telegraf.NewMetric(name, tags, fields, t)
	}

	ra.Aggregator.Apply(in)
	return ra.Config.DropOriginal
}
