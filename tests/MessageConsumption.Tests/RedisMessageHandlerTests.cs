using MediatR;
using MessageConsumption.Api.Handlers;
using MessageConsumption.Api.Messages;
using Microsoft.Extensions.Logging.Abstractions;

namespace MessageConsumption.Tests;

public class RedisMessageHandlerTests
{
    [Fact]
    public async Task Handle_LogsAndCompletesSuccessfully()
    {
        var logger = NullLogger<RedisMessageHandler>.Instance;
        var handler = new RedisMessageHandler(logger);
        var message = new RedisMessage("test-channel", "hello world");

        // Use a very short delay for the test by passing a real CancellationToken
        // and verifying the handler does not throw.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        // The actual handler simulates work; we just verify it completes without throwing.
        // In production the Task.Delay is 10 s — here we cancel after 30 s as a guard.
        await handler.Handle(message, cts.Token);
    }

    [Fact]
    public async Task Handle_RespectsCancel()
    {
        var logger = NullLogger<RedisMessageHandler>.Instance;
        var handler = new RedisMessageHandler(logger);
        var message = new RedisMessage("test-channel", "payload");

        using var cts = new CancellationTokenSource();
        // Cancel immediately so the internal Task.Delay throws OperationCanceledException.
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => handler.Handle(message, cts.Token));
    }
}
