using MediatR;
using MessageConsumption.Api.Messages;
using MessageConsumption.Api.Workers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using StackExchange.Redis;

namespace MessageConsumption.Tests;

/// <summary>
/// Tests for <see cref="RedisSubscriberWorker"/> that verify:
/// 1. Messages are dispatched concurrently (non-blocking).
/// 2. A handler failure does not prevent subsequent messages from being processed.
/// </summary>
public class RedisSubscriberWorkerTests
{
    // ── Helpers ────────────────────────────────────────────────────────────

    private static (RedisSubscriberWorker worker, IMediator mediator, Action<RedisChannel, RedisValue> capturedCallback)
        BuildWorker(string channel = "messages")
    {
        // Capture the subscription callback so tests can invoke it manually.
        Action<RedisChannel, RedisValue>? capturedCallback = null;

        var subscriber = Substitute.For<ISubscriber>();
        subscriber
            .SubscribeAsync(
                Arg.Any<RedisChannel>(),
                Arg.Do<Action<RedisChannel, RedisValue>>(cb => capturedCallback = cb),
                Arg.Any<CommandFlags>())
            .Returns(Task.CompletedTask);

        var redis = Substitute.For<IConnectionMultiplexer>();
        redis.GetSubscriber(Arg.Any<object>()).Returns(subscriber);

        // Set up a real DI scope containing a mock IMediator.
        var mediator = Substitute.For<IMediator>();
        var services = new ServiceCollection();
        services.AddSingleton(mediator);

        var scopeFactory = services.BuildServiceProvider().GetRequiredService<IServiceScopeFactory>();

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["Redis:Channel"] = channel })
            .Build();

        var logger = NullLogger<RedisSubscriberWorker>.Instance;
        var worker = new RedisSubscriberWorker(redis, scopeFactory, logger, config);

        // Execute the worker briefly so it subscribes and captures the callback.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        _ = worker.StartAsync(cts.Token);

        // Busy-wait until the callback has been captured (subscription has happened).
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (capturedCallback is null && DateTime.UtcNow < deadline)
            Thread.Sleep(10);

        return (worker, mediator, capturedCallback!);
    }

    // ── Tests ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task Worker_DispatchesMessagesToMediatR()
    {
        var (worker, mediator, callback) = BuildWorker();

        callback(RedisChannel.Literal("messages"), "payload-1");

        // Give the async dispatch a moment to complete.
        await Task.Delay(200);

        await mediator.Received(1).Publish(
            Arg.Is<RedisMessage>(m => m.Payload == "payload-1"),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Worker_ContinuesAfterHandlerException()
    {
        var (worker, mediator, callback) = BuildWorker();

        // First message causes the handler to throw.
        mediator
            .Publish(
                Arg.Is<RedisMessage>(m => m.Payload == "bad-payload"),
                Arg.Any<CancellationToken>())
            .Returns(Task.FromException(new InvalidOperationException("simulated failure")));

        // Second message should succeed normally.
        mediator
            .Publish(
                Arg.Is<RedisMessage>(m => m.Payload == "good-payload"),
                Arg.Any<CancellationToken>())
            .Returns(Task.CompletedTask);

        callback(RedisChannel.Literal("messages"), "bad-payload");
        callback(RedisChannel.Literal("messages"), "good-payload");

        await Task.Delay(300);

        // Both messages were dispatched despite the first one failing.
        await mediator.Received(1).Publish(
            Arg.Is<RedisMessage>(m => m.Payload == "bad-payload"),
            Arg.Any<CancellationToken>());

        await mediator.Received(1).Publish(
            Arg.Is<RedisMessage>(m => m.Payload == "good-payload"),
            Arg.Any<CancellationToken>());
    }

    [Fact]
    public async Task Worker_DispatchesMessagesConcurrently()
    {
        var (worker, mediator, callback) = BuildWorker();

        // Track the number of messages being processed simultaneously.
        int concurrentCount = 0;
        int maxConcurrent = 0;

        mediator
            .Publish(Arg.Any<INotification>(), Arg.Any<CancellationToken>())
            .Returns(async _ =>
            {
                int current = Interlocked.Increment(ref concurrentCount);
                int snapshot;
                do
                {
                    snapshot = maxConcurrent;
                    if (current <= snapshot) break;
                }
                while (Interlocked.CompareExchange(ref maxConcurrent, current, snapshot) != snapshot);

                await Task.Delay(150); // simulate work
                Interlocked.Decrement(ref concurrentCount);
            });

        // Fire three messages in quick succession.
        callback(RedisChannel.Literal("messages"), "msg-1");
        callback(RedisChannel.Literal("messages"), "msg-2");
        callback(RedisChannel.Literal("messages"), "msg-3");

        // Wait for all three to start (plus a small buffer).
        await Task.Delay(100);

        // All three should have been started before any finished.
        Assert.True(maxConcurrent > 1,
            $"Expected messages to be processed concurrently but max concurrent was {maxConcurrent}");
    }
}
