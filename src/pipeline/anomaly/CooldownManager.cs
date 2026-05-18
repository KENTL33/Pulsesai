using System.Collections.Concurrent;

namespace Pulses.Pipeline.Anomaly;

public sealed class CooldownManager
{
    private readonly ConcurrentDictionary<Guid, DateTimeOffset> _cooldowns = new();
    private readonly TimeSpan _defaultCooldown;

    public CooldownManager(TimeSpan? defaultCooldown = null)
        => _defaultCooldown = defaultCooldown ?? TimeSpan.FromMinutes(5);

    public bool IsInCooldown(Guid ruleId)
    {
        if (_cooldowns.TryGetValue(ruleId, out var expiresAt))
            return expiresAt > DateTimeOffset.UtcNow;
        return false;
    }

    public void StartCooldown(Guid ruleId)
        => _cooldowns[ruleId] = DateTimeOffset.UtcNow.Add(_defaultCooldown);

    public void StartCooldown(Guid ruleId, TimeSpan duration)
        => _cooldowns[ruleId] = DateTimeOffset.UtcNow.Add(duration);

    public void ClearCooldown(Guid ruleId)
        => _cooldowns.TryRemove(ruleId, out _);

    public TimeSpan? RemainingCooldown(Guid ruleId)
    {
        if (_cooldowns.TryGetValue(ruleId, out var expiresAt))
        {
            var remaining = expiresAt - DateTimeOffset.UtcNow;
            return remaining > TimeSpan.Zero ? remaining : null;
        }
        return null;
    }
}