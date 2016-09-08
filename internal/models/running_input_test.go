package models

// TODO test precisions:
// func TestDifferentPrecisions(t *testing.T) {
// 	now := time.Date(2006, time.February, 10, 12, 0, 0, 82912748, time.UTC)
// 	metrics := make(chan telegraf.Metric, 10)
// 	defer close(metrics)
// 	a := NewAccumulator(&TestMetricMaker{}, metrics)

// 	a.SetPrecision(0, time.Second)
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{"acc": "test"}, now)
// 	testm := <-a.metrics
// 	actual := testm.String()
// 	assert.Equal(t,
// 		fmt.Sprintf("acctest,acc=test value=101 %d", int64(1139572800000000000)),
// 		actual)

// 	a.SetPrecision(0, time.Millisecond)
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{"acc": "test"}, now)
// 	testm = <-a.metrics
// 	actual = testm.String()
// 	assert.Equal(t,
// 		fmt.Sprintf("acctest,acc=test value=101 %d", int64(1139572800083000000)),
// 		actual)

// 	a.SetPrecision(0, time.Microsecond)
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{"acc": "test"}, now)
// 	testm = <-a.metrics
// 	actual = testm.String()
// 	assert.Equal(t,
// 		fmt.Sprintf("acctest,acc=test value=101 %d", int64(1139572800082913000)),
// 		actual)

// 	a.SetPrecision(0, time.Nanosecond)
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{"acc": "test"}, now)
// 	testm = <-a.metrics
// 	actual = testm.String()
// 	assert.Equal(t,
// 		fmt.Sprintf("acctest,acc=test value=101 %d", int64(1139572800082912748)),
// 		actual)
// }

// func TestAddNoPrecisionWithInterval(t *testing.T) {
// 	now := time.Date(2006, time.February, 10, 12, 0, 0, 82912748, time.UTC)
// 	metrics := make(chan telegraf.Metric, 10)
// 	defer close(metrics)
// 	a := NewAccumulator(&TestMetricMaker{}, metrics)

// 	a.SetPrecision(0, time.Second)
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{})
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{"acc": "test"})
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{"acc": "test"}, now)

// 	testm := <-a.metrics
// 	actual := testm.String()
// 	assert.Contains(t, actual, "acctest value=101")

// 	testm = <-a.metrics
// 	actual = testm.String()
// 	assert.Contains(t, actual, "acctest,acc=test value=101")

// 	testm = <-a.metrics
// 	actual = testm.String()
// 	assert.Equal(t,
// 		fmt.Sprintf("acctest,acc=test value=101 %d", int64(1139572800000000000)),
// 		actual)
// }

// func TestAddNoIntervalWithPrecision(t *testing.T) {
// 	now := time.Date(2006, time.February, 10, 12, 0, 0, 82912748, time.UTC)
// 	metrics := make(chan telegraf.Metric, 10)
// 	defer close(metrics)
// 	a := NewAccumulator(&TestMetricMaker{}, metrics)

// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{})
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{"acc": "test"})
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{"acc": "test"}, now)

// 	testm := <-a.metrics
// 	actual := testm.String()
// 	assert.Contains(t, actual, "acctest value=101")

// 	testm = <-a.metrics
// 	actual = testm.String()
// 	assert.Contains(t, actual, "acctest,acc=test value=101")

// 	testm = <-a.metrics
// 	actual = testm.String()
// 	assert.Equal(t,
// 		fmt.Sprintf("acctest,acc=test value=101 %d", int64(1139572800000000000)),
// 		actual)
// }

// func TestAddDisablePrecision(t *testing.T) {
// 	now := time.Date(2006, time.February, 10, 12, 0, 0, 82912748, time.UTC)
// 	metrics := make(chan telegraf.Metric, 10)
// 	defer close(metrics)
// 	a := NewAccumulator(&TestMetricMaker{}, metrics)

// 	a.DisablePrecision()
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{})
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{"acc": "test"})
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{"acc": "test"}, now)

// 	testm := <-a.metrics
// 	actual := testm.String()
// 	assert.Contains(t, actual, "acctest value=101")

// 	testm = <-a.metrics
// 	actual = testm.String()
// 	assert.Contains(t, actual, "acctest,acc=test value=101")

// 	testm = <-a.metrics
// 	actual = testm.String()
// 	assert.Equal(t,
// 		fmt.Sprintf("acctest,acc=test value=101 %d", int64(1139572800082912748)),
// 		actual)
// }

// func TestAddGauge(t *testing.T) {
// 	now := time.Now()
// 	metrics := make(chan telegraf.Metric, 10)
// 	defer close(metrics)
// 	a := NewAccumulator(&TestMetricMaker{}, metrics)

// 	a.AddGauge("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{})
// 	a.AddGauge("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{"acc": "test"})
// 	a.AddGauge("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{"acc": "test"}, now)

// 	testm := <-metrics
// 	actual := testm.String()
// 	assert.Contains(t, actual, "acctest value=101")
// 	assert.Equal(t, testm.Type(), telegraf.Gauge)

// 	testm = <-metrics
// 	actual = testm.String()
// 	assert.Contains(t, actual, "acctest,acc=test value=101")
// 	assert.Equal(t, testm.Type(), telegraf.Gauge)

// 	testm = <-metrics
// 	actual = testm.String()
// 	assert.Equal(t,
// 		fmt.Sprintf("acctest,acc=test value=101 %d", now.UnixNano()),
// 		actual)
// 	assert.Equal(t, testm.Type(), telegraf.Gauge)
// }

// func TestAddCounter(t *testing.T) {
// 	now := time.Now()
// 	metrics := make(chan telegraf.Metric, 10)
// 	defer close(metrics)
// 	a := NewAccumulator(&TestMetricMaker{}, metrics)

// 	a.AddCounter("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{})
// 	a.AddCounter("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{"acc": "test"})
// 	a.AddCounter("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{"acc": "test"}, now)

// 	testm := <-metrics
// 	actual := testm.String()
// 	assert.Contains(t, actual, "acctest value=101")
// 	assert.Equal(t, testm.Type(), telegraf.Counter)

// 	testm = <-metrics
// 	actual = testm.String()
// 	assert.Contains(t, actual, "acctest,acc=test value=101")
// 	assert.Equal(t, testm.Type(), telegraf.Counter)

// 	testm = <-metrics
// 	actual = testm.String()
// 	assert.Equal(t,
// 		fmt.Sprintf("acctest,acc=test value=101 %d", now.UnixNano()),
// 		actual)
// 	assert.Equal(t, testm.Type(), telegraf.Counter)
// }

// func TestAddDefaultTags(t *testing.T) {
// 	now := time.Now()
// 	metrics := make(chan telegraf.Metric, 10)
// 	defer close(metrics)
// 	a := NewAccumulator(&TestMetricMaker{}, metrics)

// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{})
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{"acc": "test"})
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{"acc": "test"}, now)

// 	testm := <-metrics
// 	actual := testm.String()
// 	assert.Contains(t, actual, "acctest,default=tag value=101")

// 	testm = <-metrics
// 	actual = testm.String()
// 	assert.Contains(t, actual, "acctest,acc=test,default=tag value=101")

// 	testm = <-metrics
// 	actual = testm.String()
// 	assert.Equal(t,
// 		fmt.Sprintf("acctest,acc=test,default=tag value=101 %d", now.UnixNano()),
// 		actual)
// }

// // Test that all Inf fields get dropped, and not added to metrics channel
// func TestAddInfFields(t *testing.T) {
// 	inf := math.Inf(1)
// 	ninf := math.Inf(-1)

// 	now := time.Now()
// 	metrics := make(chan telegraf.Metric, 10)
// 	defer close(metrics)
// 	a := NewAccumulator(&TestMetricMaker{}, metrics)

// 	fields := map[string]interface{}{
// 		"usage":  inf,
// 		"nusage": ninf,
// 	}
// 	a.AddFields("acctest", fields, map[string]string{})
// 	a.AddFields("acctest", fields, map[string]string{"acc": "test"})
// 	a.AddFields("acctest", fields, map[string]string{"acc": "test"}, now)

// 	assert.Len(t, a.metrics, 0)

// 	// test that non-inf fields are kept and not dropped
// 	fields["notinf"] = float64(100)
// 	a.AddFields("acctest", fields, map[string]string{})
// 	testm := <-metrics
// 	actual := testm.String()
// 	assert.Contains(t, actual, "acctest notinf=100")
// }

// // Test that nan fields are dropped and not added
// func TestAddNaNFields(t *testing.T) {
// 	nan := math.NaN()

// 	now := time.Now()
// 	metrics := make(chan telegraf.Metric, 10)
// 	defer close(metrics)
// 	a := NewAccumulator(&TestMetricMaker{}, metrics)

// 	fields := map[string]interface{}{
// 		"usage": nan,
// 	}
// 	a.AddFields("acctest", fields, map[string]string{})
// 	a.AddFields("acctest", fields, map[string]string{"acc": "test"})
// 	a.AddFields("acctest", fields, map[string]string{"acc": "test"}, now)

// 	assert.Len(t, a.metrics, 0)

// 	// test that non-nan fields are kept and not dropped
// 	fields["notnan"] = float64(100)
// 	a.AddFields("acctest", fields, map[string]string{})
// 	testm := <-metrics
// 	actual := testm.String()
// 	assert.Contains(t, actual, "acctest notnan=100")
// }

// func TestAddUint64Fields(t *testing.T) {
// 	now := time.Now()
// 	metrics := make(chan telegraf.Metric, 10)
// 	defer close(metrics)
// 	a := NewAccumulator(&TestMetricMaker{}, metrics)

// 	fields := map[string]interface{}{
// 		"usage": uint64(99),
// 	}
// 	a.AddFields("acctest", fields, map[string]string{})
// 	a.AddFields("acctest", fields, map[string]string{"acc": "test"})
// 	a.AddFields("acctest", fields, map[string]string{"acc": "test"}, now)

// 	testm := <-metrics
// 	actual := testm.String()
// 	assert.Contains(t, actual, "acctest usage=99i")

// 	testm = <-metrics
// 	actual = testm.String()
// 	assert.Contains(t, actual, "acctest,acc=test usage=99i")

// 	testm = <-metrics
// 	actual = testm.String()
// 	assert.Equal(t,
// 		fmt.Sprintf("acctest,acc=test usage=99i %d", now.UnixNano()),
// 		actual)
// }

// func TestAddUint64Overflow(t *testing.T) {
// 	now := time.Now()
// 	metrics := make(chan telegraf.Metric, 10)
// 	defer close(metrics)
// 	a := NewAccumulator(&TestMetricMaker{}, metrics)

// 	fields := map[string]interface{}{
// 		"usage": uint64(9223372036854775808),
// 	}
// 	a.AddFields("acctest", fields, map[string]string{})
// 	a.AddFields("acctest", fields, map[string]string{"acc": "test"})
// 	a.AddFields("acctest", fields, map[string]string{"acc": "test"}, now)

// 	testm := <-metrics
// 	actual := testm.String()
// 	assert.Contains(t, actual, "acctest usage=9223372036854775807i")

// 	testm = <-metrics
// 	actual = testm.String()
// 	assert.Contains(t, actual, "acctest,acc=test usage=9223372036854775807i")

// 	testm = <-metrics
// 	actual = testm.String()
// 	assert.Equal(t,
// 		fmt.Sprintf("acctest,acc=test usage=9223372036854775807i %d", now.UnixNano()),
// 		actual)
// }

// func TestAddInts(t *testing.T) {
// 	now := time.Now()
// 	metrics := make(chan telegraf.Metric, 10)
// 	defer close(metrics)
// 	a := NewAccumulator(&TestMetricMaker{}, metrics)

// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": int(101)},
// 		map[string]string{})
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": int32(101)},
// 		map[string]string{"acc": "test"})
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": int64(101)},
// 		map[string]string{"acc": "test"}, now)

// 	testm := <-metrics
// 	actual := testm.String()
// 	assert.Contains(t, actual, "acctest,default=tag value=101i")

// 	testm = <-metrics
// 	actual = testm.String()
// 	assert.Contains(t, actual, "acctest,acc=test,default=tag value=101i")

// 	testm = <-metrics
// 	actual = testm.String()
// 	assert.Equal(t,
// 		fmt.Sprintf("acctest,acc=test,default=tag value=101i %d", now.UnixNano()),
// 		actual)
// }

// func TestAddFloats(t *testing.T) {
// 	now := time.Now()
// 	metrics := make(chan telegraf.Metric, 10)
// 	defer close(metrics)
// 	a := NewAccumulator(&TestMetricMaker{}, metrics)

// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": float32(101)},
// 		map[string]string{"acc": "test"})
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": float64(101)},
// 		map[string]string{"acc": "test"}, now)

// 	testm := <-metrics
// 	actual := testm.String()
// 	assert.Contains(t, actual, "acctest,acc=test,default=tag value=101")

// 	testm = <-metrics
// 	actual = testm.String()
// 	assert.Equal(t,
// 		fmt.Sprintf("acctest,acc=test,default=tag value=101 %d", now.UnixNano()),
// 		actual)
// }

// func TestAddStrings(t *testing.T) {
// 	now := time.Now()
// 	metrics := make(chan telegraf.Metric, 10)
// 	defer close(metrics)
// 	a := NewAccumulator(&TestMetricMaker{}, metrics)

// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": "test"},
// 		map[string]string{"acc": "test"})
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": "foo"},
// 		map[string]string{"acc": "test"}, now)

// 	testm := <-metrics
// 	actual := testm.String()
// 	assert.Contains(t, actual, "acctest,acc=test,default=tag value=\"test\"")

// 	testm = <-metrics
// 	actual = testm.String()
// 	assert.Equal(t,
// 		fmt.Sprintf("acctest,acc=test,default=tag value=\"foo\" %d", now.UnixNano()),
// 		actual)
// }

// func TestAddBools(t *testing.T) {
// 	now := time.Now()
// 	metrics := make(chan telegraf.Metric, 10)
// 	defer close(metrics)
// 	a := NewAccumulator(&TestMetricMaker{}, metrics)

// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": true}, map[string]string{"acc": "test"})
// 	a.AddFields("acctest",
// 		map[string]interface{}{"value": false}, map[string]string{"acc": "test"}, now)

// 	testm := <-metrics
// 	actual := testm.String()
// 	assert.Contains(t, actual, "acctest,acc=test,default=tag value=true")

// 	testm = <-metrics
// 	actual = testm.String()
// 	assert.Equal(t,
// 		fmt.Sprintf("acctest,acc=test,default=tag value=false %d", now.UnixNano()),
// 		actual)
// }
