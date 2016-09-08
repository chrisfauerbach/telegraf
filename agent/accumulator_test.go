package agent

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/influxdata/telegraf"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdd(t *testing.T) {
	now := time.Now()
	metrics := make(chan telegraf.Metric, 10)
	defer close(metrics)
	a := NewAccumulator(&TestMetricMaker{}, metrics)

	a.AddFields("acctest",
		map[string]interface{}{"value": float64(101)},
		map[string]string{})
	a.AddFields("acctest",
		map[string]interface{}{"value": float64(101)},
		map[string]string{"acc": "test"})
	a.AddFields("acctest",
		map[string]interface{}{"value": float64(101)},
		map[string]string{"acc": "test"}, now)

	testm := <-metrics
	actual := testm.String()
	assert.Contains(t, actual, "acctest value=101")

	testm = <-metrics
	actual = testm.String()
	assert.Contains(t, actual, "acctest,acc=test value=101")

	testm = <-metrics
	actual = testm.String()
	assert.Equal(t,
		fmt.Sprintf("acctest,acc=test value=101 %d", now.UnixNano()),
		actual)
}

func TestAddFields(t *testing.T) {
	now := time.Now()
	metrics := make(chan telegraf.Metric, 10)
	defer close(metrics)
	a := NewAccumulator(&TestMetricMaker{}, metrics)

	fields := map[string]interface{}{
		"usage": float64(99),
	}
	a.AddFields("acctest", fields, map[string]string{})
	a.AddGauge("acctest", fields, map[string]string{"acc": "test"})
	a.AddCounter("acctest", fields, map[string]string{"acc": "test"}, now)

	testm := <-metrics
	actual := testm.String()
	assert.Contains(t, actual, "acctest usage=99")

	testm = <-metrics
	actual = testm.String()
	assert.Contains(t, actual, "acctest,acc=test usage=99")

	testm = <-metrics
	actual = testm.String()
	assert.Equal(t,
		fmt.Sprintf("acctest,acc=test usage=99 %d", now.UnixNano()),
		actual)
}

func TestAccAddError(t *testing.T) {
	errBuf := bytes.NewBuffer(nil)
	log.SetOutput(errBuf)
	defer log.SetOutput(os.Stderr)

	metrics := make(chan telegraf.Metric, 10)
	defer close(metrics)
	a := NewAccumulator(&TestMetricMaker{}, metrics)

	a.AddError(fmt.Errorf("foo"))
	a.AddError(fmt.Errorf("bar"))
	a.AddError(fmt.Errorf("baz"))

	errs := bytes.Split(errBuf.Bytes(), []byte{'\n'})
	assert.EqualValues(t, 3, a.errCount)
	require.Len(t, errs, 4) // 4 because of trailing newline
	assert.Contains(t, string(errs[0]), "TestPlugin")
	assert.Contains(t, string(errs[0]), "foo")
	assert.Contains(t, string(errs[1]), "TestPlugin")
	assert.Contains(t, string(errs[1]), "bar")
	assert.Contains(t, string(errs[2]), "TestPlugin")
	assert.Contains(t, string(errs[2]), "baz")
}

type TestMetricMaker struct {
}

func (tm *TestMetricMaker) Name() string {
	return "TestPlugin"
}
func (tm *TestMetricMaker) MakeMetric(
	measurement string,
	fields map[string]interface{},
	tags map[string]string,
	mType telegraf.ValueType,
	t time.Time,
) telegraf.Metric {
	if m, err := telegraf.NewMetric(measurement, tags, fields, t); err == nil {
		return m
	}
	return nil
}
