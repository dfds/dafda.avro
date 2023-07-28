using Dafda.Configuration;
using Dafda.Avro.Consuming.Interfaces;
using Dafda.Consuming;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Avro.Specific;
using Avro;
using Dafda.Avro.Tests.TestDoubles;
using Dafda.Avro.Configuration.ConsumerConfigurations;
using Dafda.Avro.Consuming;
using Dafda.Avro.Tests.Builders;
using Newtonsoft.Json.Linq;

namespace Dafda.Avro.Tests.Configuration
{
    public class ConsumerServiceCollectionExtensionsTests
    {
        [Fact( /*Skip = "is this relevant for testing these extensions"*/)]
        public async Task Can_consume_message()
        {
            var dummyMessage = new DummyMessage();

            var messageResult = new MessageResultBuilder<string, DummyMessage>().WithKey("Test").WithValue(dummyMessage).Build();

            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<IHostApplicationLifetime, DummyApplicationLifetime>();
            services.AddAvroConsumer<string, DummyMessage>(options =>
            {
                options.WithBootstrapServers("dummyBootstrapServer");
                options.WithGroupId("dummyGroupId");
                options.WithSchemaRegistryConfig(new Confluent.SchemaRegistry.SchemaRegistryConfig());
                options.RegisterMessageHandler<DummyMessage, DummyMessageHandler>("dummyTopic");
                
                options.WithConsumerScopeFactory(_ => new ConsumerScopeFactoryStub<string, DummyMessage>(new ConsumerScopeStub<string, DummyMessage>(messageResult)));
            });
            var serviceProvider = services.BuildServiceProvider();

            var consumerHostedService = serviceProvider.GetServices<IHostedService>()
                .OfType<ConsumerHostedService>()
                .First();

            using (var cts = new CancellationTokenSource(50))
            {
                await consumerHostedService.ConsumeAll(cts.Token);
            }
            

            Assert.Equal(dummyMessage, DummyMessageHandler.LastHandledMessage);
        }

        [Fact]
        public void Add_single_consumer_registeres_a_single_hosted_service()
        {
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<IHostApplicationLifetime, DummyApplicationLifetime>();
            services.AddAvroConsumer<string, DummyMessage>(options =>
            {
                options.WithBootstrapServers("dummyBootstrapServer");
                options.WithGroupId("dummyGroupId");
                options.WithSchemaRegistryConfig(new Confluent.SchemaRegistry.SchemaRegistryConfig());
                options.RegisterMessageHandler<DummyMessage, DummyMessageHandler>("Test-Topic");
            });

            var serviceProvider = services.BuildServiceProvider();
            var consumerHostedServices = serviceProvider
                .GetServices<IHostedService>()
                .OfType<ConsumerHostedService>();

            Assert.Single(consumerHostedServices);
        }

        [Fact]
        public void Add_multiple_consumers_registeres_multiple_hosted_services()
        {
            var services = new ServiceCollection();

            services.AddLogging();
            services.AddSingleton<IHostApplicationLifetime, DummyApplicationLifetime>();

            services.AddAvroConsumer<string, DummyMessage>(options =>
            {
                options.WithBootstrapServers("dummyBootstrapServer");
                options.WithGroupId("dummyGroupId 1");
                options.WithSchemaRegistryConfig(new Confluent.SchemaRegistry.SchemaRegistryConfig());
                options.RegisterMessageHandler<DummyMessage, DummyMessageHandler>("Test-Topic");
            });

            services.AddAvroConsumer<string, DummyMessage>(options =>
            {
                options.WithBootstrapServers("dummyBootstrapServer");
                options.WithGroupId("dummyGroupId 2");
                options.WithSchemaRegistryConfig(new Confluent.SchemaRegistry.SchemaRegistryConfig());
                options.RegisterMessageHandler<DummyMessage, DummyMessageHandler>("Test-Topic");
            });

            var serviceProvider = services.BuildServiceProvider();
            var consumerHostedServices = serviceProvider
                .GetServices<IHostedService>()
                .OfType<ConsumerHostedService>();

            Assert.Equal(2, consumerHostedServices.Count());
        }

        [Fact]
        public void Throws_exception_when_registering_multiple_consumers_with_same_consumer_group_id()
        {
            var consumerGroupId = "foo";

            var services = new ServiceCollection();
            services.AddAvroConsumer<string, DummyMessage>(options =>
            {
                options.WithGroupId(consumerGroupId);
                options.WithBootstrapServers("dummy");
                options.WithSchemaRegistryConfig(new Confluent.SchemaRegistry.SchemaRegistryConfig());
                options.RegisterMessageHandler<DummyMessage, DummyMessageHandler>("Test-Topic");
            });

            Assert.Throws<InvalidConfigurationException>(() =>
            {
                services.AddAvroConsumer<string, DummyMessage>(options =>
                {
                    options.WithBootstrapServers("dummy");
                    options.WithGroupId(consumerGroupId);
                    options.WithSchemaRegistryConfig(new Confluent.SchemaRegistry.SchemaRegistryConfig());
                    options.RegisterMessageHandler<DummyMessage, DummyMessageHandler>("Test-Topic");
                });
            });
        }

        [Fact]
        public void Dose_not_throw_exception_when_registering_multiple_consumers_with_same_consumer_group_id()
        {
            var consumerGroupId = "foo";

            var services = new ServiceCollection();
            services.AddAvroConsumer<string, DummyMessage>(options =>
            {
                options.WithGroupId(consumerGroupId);
                options.WithBootstrapServers("dummy");
                options.WithSchemaRegistryConfig(new Confluent.SchemaRegistry.SchemaRegistryConfig());
                options.RegisterMessageHandler<DummyMessage, DummyMessageHandler>("Test-Topic");
            });

            services.AddAvroConsumer<string, DummyMessage>(options =>
            {
                options.WithGroupId(consumerGroupId);
                options.WithBootstrapServers("dummy");
                options.WithSchemaRegistryConfig(new Confluent.SchemaRegistry.SchemaRegistryConfig());
                options.RegisterMessageHandler<DummyMessage, DummyMessageHandler>("Test-Topic-1");
            });


        }

        [Fact]
        public async Task Default_consumer_failure_strategy_will_stop_application()
        {
            var spy = new ApplicationLifetimeSpy();
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<IHostApplicationLifetime>(_ => spy);
            services.AddAvroConsumer<string, DummyMessage>(options =>
            {
                options.WithBootstrapServers("dummyBootstrapServer");
                options.WithGroupId("dummyGroupId");
                options.WithSchemaRegistryConfig(new Confluent.SchemaRegistry.SchemaRegistryConfig());
                options.RegisterMessageHandler<DummyMessage, DummyMessageHandler>("dummyTopic");
                //                options.WithConsumerScopeFactory(_ => new FailingConsumerScopeFactory());
            });
            var serviceProvider = services.BuildServiceProvider();

            var consumerHostedService = serviceProvider.GetServices<IHostedService>()
                .OfType<ConsumerHostedService>()
                .Single();

            await consumerHostedService.ConsumeAll(CancellationToken.None);

            Assert.True(spy.StopApplicationWasCalled);
        }

        [Fact]
        public async Task Consumer_failure_strategy_is_evaluated()
        {
            const int failuresBeforeQuitting = 2;
            var count = 0;

            var spy = new ApplicationLifetimeSpy();
            var services = new ServiceCollection();
            services.AddLogging();
            services.AddSingleton<IHostApplicationLifetime>(_ => spy);
            services.AddAvroConsumer<string, DummyMessage>(options =>
            {
                options.WithBootstrapServers("dummyBootstrapServer");
                options.WithGroupId("dummyGroupId");
                //options.WithConsumerScopeFactory(_ => new FailingConsumerScopeFactory());
                options.WithSchemaRegistryConfig(new Confluent.SchemaRegistry.SchemaRegistryConfig());
                options.RegisterMessageHandler<DummyMessage, DummyMessageHandler>("dummyTopic");
                options.WithConsumerErrorHandler(exception =>
                {
                    if (++count > failuresBeforeQuitting)
                    {
                        return Task.FromResult(ConsumerFailureStrategy.Default);
                    }

                    return Task.FromResult(ConsumerFailureStrategy.RestartConsumer);
                });
            });
            var serviceProvider = services.BuildServiceProvider();

            var consumerHostedService = serviceProvider.GetServices<IHostedService>()
                .OfType<ConsumerHostedService>()
                .Single();

            await consumerHostedService.ConsumeAll(CancellationToken.None);

            Assert.Equal(failuresBeforeQuitting + 1, count);
        }

        public class DummyMessage : ISpecificRecord
        {
            public Schema Schema => throw new NotImplementedException();

            public object Get(int fieldPos)
            {
                throw new NotImplementedException();
            }

            public void Put(int fieldPos, object fieldValue)
            {
                throw new NotImplementedException();
            }
        }

        public class DummyMessage1 : ISpecificRecord
        {
            public Schema Schema => throw new NotImplementedException();

            public object Get(int fieldPos)
            {
                throw new NotImplementedException();
            }

            public void Put(int fieldPos, object fieldValue)
            {
                throw new NotImplementedException();
            }
        }

        public class DummyMessageHandler : IMessageHandler<DummyMessage>
        {
            public Task Handle(DummyMessage message, MessageHandlerContext context)
            {
                LastHandledMessage = message;

                return Task.CompletedTask;
            }

            public static object LastHandledMessage { get; private set; }
        }

        public class DummyMessageHandler1 : IMessageHandler<DummyMessage1>
        {
            public Task Handle(DummyMessage1 message, MessageHandlerContext context)
            {
                LastHandledMessage = message;

                return Task.CompletedTask;
            }

            public static object LastHandledMessage { get; private set; }
        }

        private class FailingConsumerScopeFactory : IAvroConsumerScopeFactory<MessageResult>
        {
            public IConsumerScope<MessageResult> CreateConsumerScope()
            {
                throw new System.InvalidOperationException();
            }
        }
    }
}
