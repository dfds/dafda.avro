﻿using Avro.Specific;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Dafda.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dafda.Avro.Consuming.Factories;
using Dafda.Avro.Consuming.ErrorHandlers;
using Dafda.Avro.Consuming.Interfaces;
using Dafda.Avro.Consuming;

namespace Dafda.Avro.Configuration.ConsumerConfigurations
{
    internal sealed class ConsumerConfigurationBuilderAvro<TKey, TValue> where TValue : ISpecificRecord
    {
        private static readonly string[] DefaultConfigurationKeys =
        {
            ConfigurationKey.GroupId,
            ConfigurationKey.EnableAutoCommit,
            ConfigurationKey.AllowAutoCreateTopics,
            ConfigurationKey.BootstrapServers,
            ConfigurationKey.BrokerVersionFallback,
            ConfigurationKey.ApiVersionFallbackMs,
            ConfigurationKey.SslCaLocation,
            ConfigurationKey.SaslUsername,
            ConfigurationKey.SaslPassword,
            ConfigurationKey.SaslMechanisms,
            ConfigurationKey.SecurityProtocol,
        };

        private static readonly string[] RequiredConfigurationKeys =
        {
            ConfigurationKey.GroupId,
            ConfigurationKey.BootstrapServers
        };

        private readonly IDictionary<string, string> _configurations = new Dictionary<string, string>();
        private readonly IList<NamingConvention> _namingConventions = new List<NamingConvention>();
        private Func<IServiceProvider, IAvroConsumerScopeFactory<MessageResult<TKey, TValue>>> _consumerScopeFactory;
        private ConfigurationSource _configurationSource = ConfigurationSource.Null;
        private Dafda.Consuming.IHandlerUnitOfWorkFactory _unitOfWorkFactory;
        private bool _readFromBeginning;
        private AvroSerializerConfig _searlizerConfig = null;
        private SchemaRegistryConfig _schemaRegistryConfig = null;
        private MessageRegistration<TKey, TValue> _messageRegistration = null;
        private Dafda.Consuming.Interfaces.IConsumerErrorHandler _consumerErrorHandler = AvroConsumerErrorHandler.Default;

        public ConsumerConfigurationBuilderAvro<TKey, TValue> WithConfigurationSource(ConfigurationSource configurationSource)
        {
            _configurationSource = configurationSource;
            return this;
        }

        public ConsumerConfigurationBuilderAvro<TKey, TValue> WithNamingConvention(Func<string, string> converter)
        {
            _namingConventions.Add(NamingConvention.UseCustom(converter));
            return this;
        }

        internal ConsumerConfigurationBuilderAvro<TKey, TValue> WithNamingConvention(NamingConvention namingConvention)
        {
            _namingConventions.Add(namingConvention);
            return this;
        }

        public ConsumerConfigurationBuilderAvro<TKey, TValue> WithEnvironmentStyle(string prefix = null, params string[] additionalPrefixes)
        {
            WithNamingConvention(NamingConvention.UseEnvironmentStyle(prefix));

            foreach (var additionalPrefix in additionalPrefixes)
            {
                WithNamingConvention(NamingConvention.UseEnvironmentStyle(additionalPrefix));
            }

            return this;
        }

        public ConsumerConfigurationBuilderAvro<TKey, TValue> WithConfiguration(string key, string value)
        {
            _configurations[key] = value;
            return this;
        }

        public ConsumerConfigurationBuilderAvro<TKey, TValue> WithGroupId(string groupId)
        {
            return WithConfiguration(ConfigurationKey.GroupId, groupId);
        }

        public ConsumerConfigurationBuilderAvro<TKey, TValue> WithBootstrapServers(string bootstrapServers)
        {
            return WithConfiguration(ConfigurationKey.BootstrapServers, bootstrapServers);
        }

        public ConsumerConfigurationBuilderAvro<TKey, TValue> WithUnitOfWorkFactory(Dafda.Consuming.IHandlerUnitOfWorkFactory unitOfWorkFactory)
        {
            _unitOfWorkFactory = unitOfWorkFactory;
            return this;
        }

        internal ConsumerConfigurationBuilderAvro<TKey, TValue> WithConsumerScopeFactory(Func<IServiceProvider, IAvroConsumerScopeFactory<MessageResult<TKey, TValue>>> consumerScopeFactory)
        {
            _consumerScopeFactory = consumerScopeFactory;
            return this;
        }

        public ConsumerConfigurationBuilderAvro<TKey, TValue> ReadFromBeginning()
        {
            _readFromBeginning = true;
            return this;
        }

        public ConsumerConfigurationBuilderAvro<TKey, TValue> RegisterMessageResultHandler<TMessage, TMessageHandler>(string topic)
            where TMessageHandler : Dafda.Consuming.IMessageHandler<MessageResult<TKey, TValue>> where TMessage : MessageResult<TKey, TValue>
        {
            if (_messageRegistration != null)
                throw new Exception("At the moment there is only support for one MessageHandler per consumer");

            _messageRegistration = new MessageRegistration<TKey, TValue>(topic, typeof(TMessageHandler), true);
            return this;
        }

        public ConsumerConfigurationBuilderAvro<TKey, TValue> RegisterMessageHandler<TMessage, TMessageHandler>(string topic)
            where TMessageHandler : Dafda.Consuming.IMessageHandler<TMessage> where TMessage : ISpecificRecord
        {
            if (_messageRegistration != null)
                throw new Exception("At the moment there is only support for one MessageHandler per consumer");

            _messageRegistration = new MessageRegistration<TKey, TValue>(topic, typeof(TMessageHandler), false);
            return this;
        }

        public ConsumerConfigurationBuilderAvro<TKey, TValue> WithAvroSearlizerConfig(AvroSerializerConfig config)
        {
            _searlizerConfig = config;
            return this;
        }
        public ConsumerConfigurationBuilderAvro<TKey, TValue> WithSchemaRegistryConfig(SchemaRegistryConfig config)
        {
            _schemaRegistryConfig = config;
            return this;
        }

        public ConsumerConfigurationBuilderAvro<TKey, TValue> WithConsumerErrorHandler(Func<Exception, Task<ConsumerFailureStrategy>> failureEvaluation)
        {
            _consumerErrorHandler = new AvroConsumerErrorHandler(failureEvaluation);
            return this;
        }

        internal ConsumerConfiguration<TKey, TValue> Build()
        {
            var configurations = new ConfigurationBuilder()
                .WithConfigurationKeys(DefaultConfigurationKeys)
                .WithRequiredConfigurationKeys(RequiredConfigurationKeys)
                .WithNamingConventions(_namingConventions.ToArray())
                .WithConfigurationSource(_configurationSource)
                .WithConfigurations(_configurations)
                .Build();

            if (_searlizerConfig == null)
                _searlizerConfig = new AvroSerializerConfig();

            if (_schemaRegistryConfig == null)
                throw new InvalidConfigurationException("Schema registry options not setup"); //TODO: Make this better

            if(_consumerScopeFactory == null)
            {
                Console.WriteLine("Test1");
                _consumerScopeFactory = provider =>
                {
                    var loggerFactory = provider.GetRequiredService<ILoggerFactory>();

                    return new AvroBasedConsumerScopeFactory<TKey, TValue>(
                        loggerFactory: loggerFactory,
                        configuration: configurations,
                        topic: _messageRegistration.Topic,
                        readFromBeginning: _readFromBeginning,
                        schemaRegistryConfig: _schemaRegistryConfig,
                        avroSerializerConfig: _searlizerConfig
                        );
                };
            }

            return new ConsumerConfiguration<TKey, TValue>(
                configuration: configurations,
                messageRegistration: _messageRegistration,
                unitOfWorkFactory: _unitOfWorkFactory,
                consumerScopeFactory: _consumerScopeFactory,
                consumerErrorHandler: _consumerErrorHandler
            );
        }

    }
}
