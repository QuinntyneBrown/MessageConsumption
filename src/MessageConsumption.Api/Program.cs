using MessageConsumption.Api.Workers;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// ── Logging ────────────────────────────────────────────────────────────────
builder.Logging.ClearProviders();
builder.Logging.AddConsole();

// ── API explorer / Swagger ─────────────────────────────────────────────────
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

// ── MediatR ────────────────────────────────────────────────────────────────
// Registers IMediator, ISender, IPublisher and all INotificationHandler<T>
// implementations found in this assembly.
builder.Services.AddMediatR(cfg =>
    cfg.RegisterServicesFromAssembly(typeof(Program).Assembly));

// ── Redis ──────────────────────────────────────────────────────────────────
// IConnectionMultiplexer is thread-safe and expensive to create, so register
// it as a singleton and share it across the application.
var redisConnectionString =
    builder.Configuration["Redis:ConnectionString"] ?? "localhost:6379";

builder.Services.AddSingleton<IConnectionMultiplexer>(_ =>
    ConnectionMultiplexer.Connect(redisConnectionString));

// ── Background worker ──────────────────────────────────────────────────────
// Subscribes to the configured Redis Pub/Sub channel and dispatches each
// incoming message to MediatR concurrently (non-blocking).
builder.Services.AddHostedService<RedisSubscriberWorker>();

// ── Build ──────────────────────────────────────────────────────────────────
var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

// ── Health / status endpoint ───────────────────────────────────────────────
app.MapGet("/status", () => Results.Ok(new { status = "running" }))
   .WithName("GetStatus")
   .WithOpenApi();

app.Run();
