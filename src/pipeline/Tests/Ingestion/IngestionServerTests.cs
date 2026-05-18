using System.Threading.Channels;
using Pulses.Pipeline.Ingestion;
using Pulses.Shared;

namespace Pulses.Pipeline.Tests.Ingestion;

public class IngestionServerTests
{
    [Fact]
    public async Task EnqueueEvents_ShouldQueueToChannel()
    {
        var channel = new EventChannel();
        var server = new IngestionServer(channel);

        var evt = new SensorEvent { SensorId = Guid.NewGuid(), Value = 1.0, Timestamp = DateTimeOffset.UtcNow };
        server.Enqueue(evt);

        await Task.Delay(100);
        Assert.True(channel.Reader.TryRead(out var result));
        Assert.Equal(evt.SensorId, result.SensorId);
        Assert.Equal(1.0, result.Value);
    }

    [Fact]
    public async Task EnqueueBatch_ShouldQueueAllEvents()
    {
        var channel = new EventChannel();
        var server = new IngestionServer(channel);

        var batch = Enumerable.Range(0, 50).Select(i => new SensorEvent
        {
            SensorId = Guid.NewGuid(),
            Value = i * 1.0,
            Timestamp = DateTimeOffset.UtcNow,
        }).ToList();

        server.EnqueueBatch(batch);

        await Task.Delay(200);
        int count = 0;
        while (channel.Reader.TryRead(out _)) count++;
        Assert.Equal(50, count);
    }

    [Fact]
    public async Task Enqueue_WhenChannelFull_ShouldDropOldest()
    {
        var channel = new EventChannel(capacity: 10);
        var server = new IngestionServer(channel);

        // Fill with 10 events
        for (int i = 0; i < 10; i++)
        {
            server.Enqueue(new SensorEvent
            {
                SensorId = Guid.NewGuid(),
                Value = i,
                Timestamp = DateTimeOffset.UtcNow,
                Sequence = i,
            });
        }

        // Add 11th — should drop the oldest (sequence 0)
        server.Enqueue(new SensorEvent
        {
            SensorId = Guid.NewGuid(),
            Value = 100,
            Timestamp = DateTimeOffset.UtcNow,
            Sequence = 10,
        });

        await Task.Delay(100);

        var remaining = new List<double>();
        while (channel.Reader.TryRead(out var e)) remaining.Add(e.Value);

        Assert.DoesNotContain(0.0, remaining);
        Assert.Contains(1.0, remaining);
        Assert.Contains(100.0, remaining);
    }
}