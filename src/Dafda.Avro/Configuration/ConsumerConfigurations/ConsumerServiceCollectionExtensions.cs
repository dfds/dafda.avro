using Avro.Specific;
using Dafda.Avro.Consuming;
using Dafda.Configuration;
using Dafda.Consuming;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Confluent.Kafka.ConfigPropertyNames;

namespace Dafda.Avro.Configuration.ConsumerConfigurations
{
    public static class ConsumerServiceCollectionExtensions
    {
        private class ConsumerGroupIdRepository
        {
            private readonly ISet<KeyValuePair<string,string>> _topics;

            public ConsumerGroupIdRepository()
            {
                _topics = new HashSet<KeyValuePair<string, string>>();
            }

            public void Add(string newId, string topic) => _topics.Add(new KeyValuePair<string, string>(newId, topic));
            public bool Contains(string id, string topic) => _topics.Contains(new KeyValuePair<string, string>(id, topic));
        }

        /// <summary>
        /// Add asingle Avro Kafka consumer. The consumer will run in an <see cref="IHostedService"/>.
        /// </summary>
        /// <typeparam name="TKey"></typeparam>
        /// <typeparam name="TValue"></typeparam>
        /// <param name="services"></param>
        /// <param name="options"></param>
        /// <exception cref="InvalidConfigurationException"></exception>
        public static void AddAvroConsumer<TKey, TValue>(this IServiceCollection services, Action<ConsumerOptionsAvro<TKey, TValue>> options = null) where TValue : ISpecificRecord
        {
            var configBuilder = new ConsumerConfigurationBuilderAvro<TKey, TValue>();
            var consumerOptions = new ConsumerOptionsAvro<TKey, TValue>(configBuilder, services);
            consumerOptions.WithUnitOfWorkFactory<ServiceProviderUnitOfWorkFactory>();
            consumerOptions.WithUnconfiguredMessageHandlingStrategy<RequireExplicitHandlers>();
            options?.Invoke(consumerOptions);
            var configuration = configBuilder.Build();

            var consumerGroupIdRepository = services
                .Where(x => x.ServiceType == typeof(ConsumerGroupIdRepository))
                .Where(x => x.Lifetime == ServiceLifetime.Singleton)
                .Where(x => x.ImplementationInstance != null)
                .Where(x => x.ImplementationInstance.GetType() == typeof(ConsumerGroupIdRepository))
                .Select(x => x.ImplementationInstance)
                .Cast<ConsumerGroupIdRepository>()
                .FirstOrDefault();

            if (consumerGroupIdRepository == null)
            {
                consumerGroupIdRepository = new ConsumerGroupIdRepository();
                services.AddSingleton(consumerGroupIdRepository);
            }
            
            if (consumerGroupIdRepository.Contains(configuration.GroupId, configuration.MessageRegistration.Topic))
                throw new InvalidConfigurationException($"Multiple consumers CANNOT be configured with same consumer group id \"{configuration.GroupId}\".");

            consumerGroupIdRepository.Add(configuration.GroupId, configuration.MessageRegistration.Topic);


            ConsumerHostedService HostedServiceFactory(IServiceProvider provider) => new ConsumerHostedService(
                logger: provider.GetRequiredService<ILogger<ConsumerHostedService>>(),
                applicationLifetime: provider.GetRequiredService<IHostApplicationLifetime>(),
                consumer: new AvroConsumer<TKey, TValue>(
                            messageHandler: configuration.MessageRegistration,
                            unitOfWorkFactory: provider.GetRequiredService<IHandlerUnitOfWorkFactory>(),
                            consumerScopeFactory: configuration.AvroConsumerScopeFactory(provider),
                            isAutoCommitEnabled: configuration.EnableAutoCommit),
                groupId: configuration.GroupId,
                consumerErrorHandler: configuration.ConsumerErrorHandler
                );

            services.AddTransient<IHostedService, ConsumerHostedService>(HostedServiceFactory);
            services.AddTransient(HostedServiceFactory); // NOTE: [jandr] is this needed?
        }
    }
}
