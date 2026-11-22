using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NexusMQ.Client;

namespace NexusMQ.AspNetCore;

/// <summary>Handle messages from a queue or subscription. Resolved from DI in a new scope per message (inject DbContext etc.).</summary>
public interface INexusMessageHandler
{
    Task HandleAsync(ProcessMessageEventArgs args);
}

/// <summary>Handle messages whose body is JSON of type <typeparamref name="T"/>.</summary>
public interface INexusMessageHandler<in T>
{
    Task HandleAsync(T message, ProcessMessageEventArgs args);
}

/// <summary>Handle messages from a session-enabled entity (one session at a time per slot, in order).</summary>
public interface INexusSessionMessageHandler
{
    Task HandleAsync(ProcessSessionMessageEventArgs args);
}

public static class NexusClientExtensions
{
    /// <summary>Register a singleton <see cref="NexusClient"/> (and its <see cref="NexusAdminClient"/>) for a NexusMQ server.</summary>
    public static IServiceCollection AddNexusMQClient(this IServiceCollection services, string baseUrl, string? apiKey = null)
    {
        services.AddSingleton(_ => new NexusClient(baseUrl, apiKey));
        services.AddSingleton(sp => sp.GetRequiredService<NexusClient>().Admin);
        return services;
    }

    /// <summary>Run <typeparamref name="THandler"/> in the background for every message on <paramref name="entityPath"/>.</summary>
    public static IServiceCollection AddNexusMQHandler<THandler>(this IServiceCollection services, string entityPath,
        Action<NexusProcessorOptions>? configure = null)
        where THandler : class, INexusMessageHandler
    {
        services.AddScoped<THandler>();
        var options = new NexusProcessorOptions();
        configure?.Invoke(options);
        services.AddSingleton<IHostedService>(sp => new NexusHandlerService(sp, entityPath, options,
            (scope, args) => scope.GetRequiredService<THandler>().HandleAsync(args), typeof(THandler).Name));
        return services;
    }

    /// <summary>Run <typeparamref name="THandler"/> for every message on <paramref name="entityPath"/>, with the JSON body deserialized to <typeparamref name="TMessage"/>.</summary>
    public static IServiceCollection AddNexusMQHandler<THandler, TMessage>(this IServiceCollection services, string entityPath,
        Action<NexusProcessorOptions>? configure = null)
        where THandler : class, INexusMessageHandler<TMessage>
    {
        services.AddScoped<THandler>();
        var options = new NexusProcessorOptions();
        configure?.Invoke(options);
        services.AddSingleton<IHostedService>(sp => new NexusHandlerService(sp, entityPath, options,
            (scope, args) => scope.GetRequiredService<THandler>().HandleAsync(
                args.Message.BodyAs<TMessage>() ?? throw new InvalidOperationException("Message body was null."), args),
            typeof(THandler).Name));
        return services;
    }

    /// <summary>Run <typeparamref name="THandler"/> for a session-enabled queue or subscription.</summary>
    public static IServiceCollection AddNexusMQSessionHandler<THandler>(this IServiceCollection services, string entityPath,
        Action<NexusSessionProcessorOptions>? configure = null)
        where THandler : class, INexusSessionMessageHandler
    {
        services.AddScoped<THandler>();
        var options = new NexusSessionProcessorOptions();
        configure?.Invoke(options);
        services.AddSingleton<IHostedService>(sp => new NexusSessionHandlerService<THandler>(sp, entityPath, options));
        return services;
    }

    private sealed class NexusHandlerService(
        IServiceProvider services,
        string entityPath,
        NexusProcessorOptions options,
        Func<IServiceProvider, ProcessMessageEventArgs, Task> invoke,
        string handlerName) : IHostedService, IAsyncDisposable
    {
        private NexusProcessor? _processor;

        public Task StartAsync(CancellationToken ct)
        {
            var logger = services.GetRequiredService<ILoggerFactory>().CreateLogger("NexusMQ." + handlerName);
            _processor = services.GetRequiredService<NexusClient>().CreateProcessor(entityPath, options);
            _processor.ProcessMessageAsync += async args =>
            {
                await using var scope = services.CreateAsyncScope();
                await invoke(scope.ServiceProvider, args);
            };
            _processor.ProcessErrorAsync += args =>
            {
                logger.LogWarning(args.Exception, "NexusMQ handler {Handler} on {Entity} failed", handlerName, args.EntityPath);
                return Task.CompletedTask;
            };
            return _processor.StartProcessingAsync(ct);
        }

        public Task StopAsync(CancellationToken ct) => _processor?.StopProcessingAsync() ?? Task.CompletedTask;

        public async ValueTask DisposeAsync()
        {
            if (_processor is not null) await _processor.DisposeAsync();
        }
    }

    private sealed class NexusSessionHandlerService<THandler>(IServiceProvider services, string entityPath, NexusSessionProcessorOptions options)
        : IHostedService, IAsyncDisposable
        where THandler : class, INexusSessionMessageHandler
    {
        private NexusSessionProcessor? _processor;

        public Task StartAsync(CancellationToken ct)
        {
            var logger = services.GetRequiredService<ILoggerFactory>().CreateLogger("NexusMQ." + typeof(THandler).Name);
            _processor = services.GetRequiredService<NexusClient>().CreateSessionProcessor(entityPath, options);
            _processor.ProcessMessageAsync += async args =>
            {
                await using var scope = services.CreateAsyncScope();
                await scope.ServiceProvider.GetRequiredService<THandler>().HandleAsync(args);
            };
            _processor.ProcessErrorAsync += args =>
            {
                logger.LogWarning(args.Exception, "NexusMQ session handler {Handler} on {Entity} failed", typeof(THandler).Name, args.EntityPath);
                return Task.CompletedTask;
            };
            return _processor.StartProcessingAsync(ct);
        }

        public Task StopAsync(CancellationToken ct) => _processor?.StopProcessingAsync() ?? Task.CompletedTask;

        public async ValueTask DisposeAsync()
        {
            if (_processor is not null) await _processor.DisposeAsync();
        }
    }
}
