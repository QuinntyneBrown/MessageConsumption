using MediatR;
using MessageConsumption.Api.Messages;

namespace MessageConsumption.Api.Handlers;

/// <summary>
/// Handles messages received from Redis Pub/Sub.
/// Replace the body of this handler with your business logic.
/// </summary>
public class RedisMessageHandler : INotificationHandler<RedisMessage>
{
    private readonly ILogger<RedisMessageHandler> _logger;

    public RedisMessageHandler(ILogger<RedisMessageHandler> logger)
    {
        _logger = logger;
    }

    public async Task Handle(RedisMessage notification, CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Processing message from channel '{Channel}': {Payload}",
            notification.Channel,
            notification.Payload);

        // Simulate work (e.g. 10 seconds of processing).
        // Because the worker dispatches each message via fire-and-forget, this does
        // NOT block the subscriber loop — new messages are accepted immediately.
        await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);

        _logger.LogInformation(
            "Finished processing message from channel '{Channel}'",
            notification.Channel);
    }
}
