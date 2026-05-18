using System.Threading.Channels;
using Pulses.Shared;

namespace Pulses.Pipeline.Ingestion;

public sealed class EventChannel
{
    private readonly Channel<SensorEvent> _channel;
    private readonly int _capacity;

    public EventChannel(int capacity = 10000)
    {
        _capacity = capacity;
        _channel = System.Threading.Channels.Channel.CreateBounded<SensorEvent>(new BoundedChannelOptions(capacity)
        {
            FullMode = BoundedChannelFullMode.DropOldest, // Oldest events drop when full; producers never block
            SingleReader = true,
            SingleWriter = false,
        });
    }

    public bool TryWrite(SensorEvent evt) => _channel.Writer.TryWrite(evt);

    public ValueTask WriteAsync(SensorEvent evt, CancellationToken ct = default)
        => _channel.Writer.WriteAsync(evt, ct);

    public Channel<SensorEvent> Channel => _channel;

    public ChannelReader<SensorEvent> Reader => _channel.Reader;

    public int Capacity => _capacity;

    public int Count => _channel.Reader.Count;
}