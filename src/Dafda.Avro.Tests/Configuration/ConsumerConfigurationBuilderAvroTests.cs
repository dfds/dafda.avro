using Avro;
using Avro.Specific;
using Dafda.Avro.Configuration.ConsumerConfigurations;
using Dafda.Avro.Tests.TestDoubles;
using Dafda.Configuration;
using Dafda.Consuming;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Dafda.Avro.Tests.Configuration
{
    public class ConsumerConfigurationBuilderAvroTest
    {
        [Fact]
        public void Can_validate_configuration()
        {
            var sut = new ConsumerConfigurationBuilderAvro<string, DummyMessage>();

            Assert.Throws<InvalidConfigurationException>(() => sut.Build());
        }

        [Fact]
        public void Can_build_minimal_configuration()
        {
            var configuration = new ConsumerConfigurationBuilderAvro<string, DummyMessage>()
                .WithGroupId("foo")
                .WithBootstrapServers("bar")
                .WithSchemaRegistryConfig(new Confluent.SchemaRegistry.SchemaRegistryConfig())
                .Build();

            AssertKeyValue(configuration, ConfigurationKey.GroupId, "foo");
            AssertKeyValue(configuration, ConfigurationKey.BootstrapServers, "bar");
        }

        private static void AssertKeyValue(ConsumerConfiguration<string, DummyMessage> configuration, string expectedKey, string expectedValue)
        {
            configuration.KafkaConfiguration.FirstOrDefault(x => x.Key == expectedKey).Deconstruct(out _, out var actualValue);

            Assert.Equal(expectedValue, actualValue);
        }

        [Fact]
        public void Can_build_consumer_configuration()
        {
            var configuration = new ConsumerConfigurationBuilderAvro<string, DummyMessage>()
                .WithConfigurationSource(new ConfigurationSourceStub(
                    (key: "DEFAULT_KAFKA_GROUP_ID", value: "default_foo"),
                    (key: "SAMPLE_KAFKA_ENABLE_AUTO_COMMIT", value: "true"),
                    (key: "SAMPLE_KAFKA_ALLOW_AUTO_CREATE_TOPICS", value: "false"),
                    (key: ConfigurationKey.GroupId, value: "foo"),
                    (key: ConfigurationKey.BootstrapServers, value: "bar"),
                    (key: "dummy", value: "ignored")

                ))
                .WithNamingConvention(NamingConvention.Default)
                .WithEnvironmentStyle("DEFAULT_KAFKA", "SAMPLE_KAFKA")
                .WithConfiguration(ConfigurationKey.GroupId, "baz")
                .WithSchemaRegistryConfig(new Confluent.SchemaRegistry.SchemaRegistryConfig())
                .Build();

            AssertKeyValue(configuration, ConfigurationKey.GroupId, "baz");
            AssertKeyValue(configuration, ConfigurationKey.BootstrapServers, "bar");
            AssertKeyValue(configuration, ConfigurationKey.EnableAutoCommit, "true");
            AssertKeyValue(configuration, ConfigurationKey.AllowAutoCreateTopics, "false");
            AssertKeyValue(configuration, "dummy", null);
        }

        [Fact]
        public void Can_register_message_handler()
        {
            var configuration = new ConsumerConfigurationBuilderAvro<string, DummyMessage>()
                .WithGroupId("foo")
                .WithBootstrapServers("bar")
                .WithSchemaRegistryConfig(new Confluent.SchemaRegistry.SchemaRegistryConfig())
                .RegisterMessageHandler<DummyMessage, DummyMessageHandler>("dummyTopic")
                .Build();

            
            var registration = configuration.MessageRegistration;

            Assert.Equal(typeof(DummyMessageHandler), registration.HandlerInstanceType);
        }

        [Fact]
        public void Returns_expected_auto_commit_when_not_set()
        {
            var configuration = new ConsumerConfigurationBuilderAvro<string, DummyMessage>()
                .WithGroupId("foo")
                .WithSchemaRegistryConfig(new Confluent.SchemaRegistry.SchemaRegistryConfig())
                .WithBootstrapServers("bar")
                .Build();

            Assert.True(configuration.EnableAutoCommit);
        }

        [Theory]
        [InlineData("true", true)]
        [InlineData("TRUE", true)]
        [InlineData("false", false)]
        [InlineData("FALSE", false)]
        public void Returns_expected_auto_commit_when_configured_with_valid_value(string configValue, bool expected)
        {
            var configuration = new ConsumerConfigurationBuilderAvro<string, DummyMessage>()
                .WithGroupId("foo")
                .WithBootstrapServers("bar")
                .WithConfiguration(ConfigurationKey.EnableAutoCommit, configValue)
                .WithSchemaRegistryConfig(new Confluent.SchemaRegistry.SchemaRegistryConfig())
                .Build();

            Assert.Equal(expected, configuration.EnableAutoCommit);
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

        // ReSharper disable once MemberCanBePrivate.Global
        private class DummyMessageHandler : IMessageHandler<DummyMessage>
        {
            public Task Handle(DummyMessage message, MessageHandlerContext context)
            {
                LastHandledMessage = message;

                return Task.CompletedTask;
            }

            public object LastHandledMessage { get; private set; }
        }

    }
}
