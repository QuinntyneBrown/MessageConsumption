using MediatR;

namespace MessageConsumption.Api.Messages;

/// <summary>
/// Represents a message received from a Redis Pub/Sub channel.
/// </summary>
public record RedisMessage(string Channel, string Payload) : INotification;
