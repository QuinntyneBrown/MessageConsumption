using MediatR;
using MessageConsumption.Api.Messages;
using StackExchange.Redis;

namespace MessageConsumption.Api.Workers;

/// <summary>
/// A hosted background service that subscribes to a Redis Pub/Sub channel and
/// dispatches each incoming message to a MediatR handler concurrently.
///
/// Key design properties:
///  - Non-blocking: the subscription callback fires a Task and returns immediately,
///    so the Redis subscriber loop is never held up by message-processing work.
///  - Concurrent: multiple messages can be processed simultaneously.
///  - Fault-tolerant: exceptions inside a handler are caught and logged; the
///    subscriber loop continues running for subsequent messages.
/// </summary>
public class RedisSubscriberWorker : BackgroundService
{
    private readonly IConnectionMultiplexer _redis;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<RedisSubscriberWorker> _logger;
    private readonly string _channel;

    public RedisSubscriberWorker(
        IConnectionMultiplexer redis,
        IServiceScopeFactory scopeFactory,
        ILogger<RedisSubscriberWorker> logger,
        IConfiguration configuration)
    {
        _redis = redis;
        _scopeFactory = scopeFactory;
        _logger = logger;
        _channel = configuration["Redis:Channel"] ?? "messages";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var subscriber = _redis.GetSubscriber();

        _logger.LogInformation("Subscribing to Redis channel '{Channel}'", _channel);

        await subscriber.SubscribeAsync(
            RedisChannel.Literal(_channel),
            (channel, value) =>
            {
                // Fire-and-forget: dispatch to handler without awaiting.
                // This keeps the subscriber callback non-blocking so that the
                // next message can be accepted immediately.
                _ = HandleMessageAsync(channel, value, stoppingToken);
            });

        _logger.LogInformation("Subscribed to Redis channel '{Channel}'. Waiting for messages…", _channel);

        // Keep the service alive until a cancellation is requested.
        try
        {
            await Task.Delay(Timeout.Infinite, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown path — swallow so we can unsubscribe cleanly.
        }

        _logger.LogInformation("Unsubscribing from Redis channel '{Channel}'", _channel);
        await subscriber.UnsubscribeAsync(RedisChannel.Literal(_channel));
    }

    /// <summary>
    /// Processes a single Redis message inside its own DI scope.
    /// Any exception is caught and logged so that the subscriber loop
    /// continues handling subsequent messages unaffected.
    /// </summary>
    private async Task HandleMessageAsync(
        RedisChannel channel,
        RedisValue value,
        CancellationToken stoppingToken)
    {
        try
        {
            // Create a fresh DI scope for each message so that scoped services
            // (e.g. DbContext) are properly isolated per-message.
            using var scope = _scopeFactory.CreateScope();
            var mediator = scope.ServiceProvider.GetRequiredService<IMediator>();

            var message = new RedisMessage(channel.ToString(), value.ToString());

            _logger.LogDebug(
                "Dispatching message from channel '{Channel}' to MediatR",
                channel);

            await mediator.Publish(message, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            // Shutdown in progress — stop processing gracefully.
        }
        catch (Exception ex)
        {
            // Log and swallow so the subscriber loop remains operational.
            _logger.LogError(
                ex,
                "Unhandled exception while processing message from channel '{Channel}'",
                channel);
        }
    }
}
