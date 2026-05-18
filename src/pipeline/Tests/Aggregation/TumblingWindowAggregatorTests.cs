using Pulses.Pipeline.Aggregation;
using Pulses.Shared;

namespace Pulses.Pipeline.Tests.Aggregation;

public class TumblingWindowAggregatorTests
{
    [Fact]
    public void Process_SingleEvent_AccumulatesCorrectly()
    {
        var flushed = new List<AggregatedMetric>();
        var aggregator = new TumblingWindowAggregator(windowSizeMs: 5000, _ => { });

        var sensorId = Guid.NewGuid();
        var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        aggregator.Process(new SensorEvent { SensorId = sensorId, Value = 42.0, Timestamp = ts });

        var snapshot = aggregator.TakeSnapshot();
        Assert.Single(snapshot);
        var metric = snapshot[0];
        Assert.Equal(sensorId, metric.SensorId);
        Assert.Equal(42.0, metric.AvgValue);
        Assert.Equal(42.0, metric.MinValue);
        Assert.Equal(42.0, metric.MaxValue);
        Assert.Equal(1, metric.Count);
    }

    [Fact]
    public void Process_MultipleEventsForSameSensor_ComputesCorrectStats()
    {
        var aggregator = new TumblingWindowAggregator(windowSizeMs: 5000, _ => { });
        var sensorId = Guid.NewGuid();
        var baseTs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        aggregator.Process(new SensorEvent { SensorId = sensorId, Value = 10.0, Timestamp = baseTs });
        aggregator.Process(new SensorEvent { SensorId = sensorId, Value = 20.0, Timestamp = baseTs });
        aggregator.Process(new SensorEvent { SensorId = sensorId, Value = 30.0, Timestamp = baseTs });

        var snapshot = aggregator.TakeSnapshot();
        Assert.Single(snapshot);
        var metric = snapshot[0];
        Assert.Equal(20.0, metric.AvgValue);
        Assert.Equal(10.0, metric.MinValue);
        Assert.Equal(30.0, metric.MaxValue);
        Assert.Equal(3, metric.Count);
    }

    [Fact]
    public void Process_DifferentSensors_AreTrackedSeparately()
    {
        var aggregator = new TumblingWindowAggregator(windowSizeMs: 5000, _ => { });
        var sensorA = Guid.NewGuid();
        var sensorB = Guid.NewGuid();
        var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        aggregator.Process(new SensorEvent { SensorId = sensorA, Value = 100.0, Timestamp = ts });
        aggregator.Process(new SensorEvent { SensorId = sensorB, Value = 200.0, Timestamp = ts });

        var snapshot = aggregator.TakeSnapshot();
        Assert.Equal(2, snapshot.Count);
        Assert.Contains(snapshot, m => m.SensorId == sensorA && m.AvgValue == 100.0);
        Assert.Contains(snapshot, m => m.SensorId == sensorB && m.AvgValue == 200.0);
    }

    [Fact]
    public void Process_WindowRollover_FlushesCompletedWindow()
    {
        AggregatedMetric? flushed = null;
        var aggregator = new TumblingWindowAggregator(windowSizeMs: 5000, batch =>
        {
            flushed = batch.Single();
        });

        var sensorId = Guid.NewGuid();
        var ts1 = 1_000_000_000_000L; // window start
        aggregator.Process(new SensorEvent { SensorId = sensorId, Value = 1.0, Timestamp = ts1 });

        var ts2 = ts1 + 5000; // new window
        aggregator.Process(new SensorEvent { SensorId = sensorId, Value = 2.0, Timestamp = ts2 });

        Assert.NotNull(flushed);
        Assert.Equal(1.0, flushed!.AvgValue);
        Assert.Equal(1, flushed.Count);
    }

    [Fact]
    public void TakeSnapshot_DoesNotMutateState()
    {
        var aggregator = new TumblingWindowAggregator(windowSizeMs: 5000, _ => { });
        var sensorId = Guid.NewGuid();
        var ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        aggregator.Process(new SensorEvent { SensorId = sensorId, Value = 5.0, Timestamp = ts });

        var snap1 = aggregator.TakeSnapshot();
        var snap2 = aggregator.TakeSnapshot();
        var snap3 = aggregator.TakeSnapshot();

        Assert.Equal(snap1[0].AvgValue, snap2[0].AvgValue);
        Assert.Equal(snap2[0].Count, snap3[0].Count);
    }
}