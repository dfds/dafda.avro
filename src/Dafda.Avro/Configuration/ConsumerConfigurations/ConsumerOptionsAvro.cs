﻿using Avro.Specific;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Dafda.Configuration;
using Dafda.Consuming;
using Dafda.Avro.Consuming.Interfaces;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dafda.Avro.Configuration.ConsumerConfigurations
{
    /// <summary>
    /// Facilitates Dafda configuration in .NET applications using the <see cref="IServiceCollection"/>.
    /// </summary>
    public sealed class ConsumerOptionsAvro<TKey, TValue> where TValue : ISpecificRecord
    {
        private readonly ConsumerConfigurationBuilderAvro<TKey, TValue> _builder;
        private readonly IServiceCollection _services;

        internal ConsumerOptionsAvro(ConsumerConfigurationBuilderAvro<TKey, TValue> builder, IServiceCollection services)
        {
            _services = services;
            _builder = builder;
        }

        /// <summary>
        /// Sets the partion offset for all subscribed topics to the beginning
        /// </summary>
        public void ReadFromBeginningOfTopics()
        {
            _builder.ReadFromBeginning();
        }

        /// <summary>
        /// Specify a custom implementation of the <see cref="ConfigurationSource"/> to use. 
        /// </summary>
        /// <param name="configurationSource">The <see cref="ConfigurationSource"/> to use.</param>
        public void WithConfigurationSource(ConfigurationSource configurationSource)
        {
            _builder.WithConfigurationSource(configurationSource);
        }

        /// <summary>
        /// Use <see cref="Microsoft.Extensions.Configuration.IConfiguration"/> as the configuration source.
        /// </summary>
        /// <param name="configuration">The configuration instance.</param>
        public void WithConfigurationSource(Microsoft.Extensions.Configuration.IConfiguration configuration)
        {
            _builder.WithConfigurationSource(new DefaultConfigurationSource(configuration));
        }

        /// <summary>
        /// Add a custom naming convention for converting configuration keys when
        /// looking up keys in the <see cref="ConfigurationSource"/>.
        /// </summary>
        /// <param name="converter">Use this to transform keys.</param>
        public void WithNamingConvention(Func<string, string> converter)
        {
            _builder.WithNamingConvention(converter);
        }

        /// <summary>
        /// Add default environment style naming convention. The configuration will attempt to
        /// fetch keys from <see cref="ConfigurationSource"/>, using the following scheme:
        /// <list type="bullet">
        ///     <item><description>keys will be converted to uppercase.</description></item>
        ///     <item><description>any one or more of <c>SPACE</c>, <c>TAB</c>, <c>.</c>, and <c>-</c> will be converted to a single <c>_</c>.</description></item>
        ///     <item><description>the prefix will be prefixed (in uppercase) along with a <c>_</c>.</description></item>
        /// </list>
        /// 
        /// When configuring a consumer the <c>WithEnvironmentStyle("app")</c>, Dafda will attempt to find the
        /// key <c>APP_GROUP_ID</c> in the <see cref="ConfigurationSource"/>.
        /// </summary>
        /// <param name="prefix">The prefix to use before keys.</param>
        /// <param name="additionalPrefixes">Additional prefixes to use before keys.</param>
        public void WithEnvironmentStyle(string prefix = null, params string[] additionalPrefixes)
        {
            _builder.WithEnvironmentStyle(prefix, additionalPrefixes);
        }

        /// <summary>
        /// Add a configuration key/value directly to the underlying Kafka consumer.
        /// </summary>
        /// <param name="key">The configuration key.</param>
        /// <param name="value">The configuration value.</param>
        public void WithConfiguration(string key, string value)
        {
            _builder.WithConfiguration(key, value);
        }

        /// <summary>
        /// A shorthand to set the <c>group.id</c> Kafka configuration value.
        /// </summary>
        /// <param name="groupId">The group id for the consumer.</param>
        public void WithGroupId(string groupId)
        {
            _builder.WithGroupId(groupId);
        }

        /// <summary>
        /// A shorthand to set the <c>bootstrap.servers</c> Kafka configuration value.
        /// </summary>
        /// <param name="bootstrapServers">A list of bootstrap servers.</param>
        public void WithBootstrapServers(string bootstrapServers)
        {
            _builder.WithBootstrapServers(bootstrapServers);
        }

        /// <summary>
        /// Override the default Dafda implementation of <see cref="IHandlerUnitOfWorkFactory"/>.
        /// </summary>
        /// <typeparam name="T">A custom implementation of <see cref="IHandlerUnitOfWorkFactory"/>.</typeparam>
        public void WithUnitOfWorkFactory<T>() where T : class, IHandlerUnitOfWorkFactory
        {
            _services.AddTransient<IHandlerUnitOfWorkFactory, T>();
        }

        internal void WithConsumerScopeFactory(Func<IServiceProvider, IAvroConsumerScopeFactory<Consuming.MessageResult<TKey, TValue>>> consumerScopeFactory)
        {
            _builder.WithConsumerScopeFactory(consumerScopeFactory);
        }

        /// <summary>
        /// Override the default Dafda implementation of <see cref="IHandlerUnitOfWorkFactory"/>.
        /// </summary>
        /// <param name="implementationFactory">The factory that creates the instance of <see cref="IHandlerUnitOfWorkFactory"/>.</param>
        public void WithUnitOfWorkFactory(Func<IServiceProvider, IHandlerUnitOfWorkFactory> implementationFactory)
        {
            _services.AddTransient(implementationFactory);
        }

        /// <summary>
        /// Register a message handler for on <paramref name="topic"/>. The
        /// specified <typeparamref name="TMessageHandler"/> must implements <see cref="IMessageHandler{T}"/>
        /// closing on the <typeparamref name="TMessage"/> type.
        /// </summary>
        /// <typeparam name="TMessage">The message type.</typeparam>
        /// <typeparam name="TMessageHandler">The message handler.</typeparam>
        /// <param name="topic">The name of the topic in Kafka.</param>
        public void RegisterMessageResultHandler<TMessage, TMessageHandler>(string topic)
            where TMessageHandler : class, IMessageHandler<Consuming.MessageResult<TKey, TValue>> where TMessage : Consuming.MessageResult<TKey, TValue>
        {
            _builder.RegisterMessageResultHandler<TMessage, TMessageHandler>(topic);
            _services.AddTransient<TMessageHandler>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="TMessage"></typeparam>
        /// <typeparam name="TMessageHandler"></typeparam>
        /// <param name="topic"></param>
        public void RegisterMessageHandler<TMessage, TMessageHandler>(string topic)
            where TMessageHandler : class, IMessageHandler<TMessage> where TMessage : ISpecificRecord
        {
            _builder.RegisterMessageHandler<TMessage, TMessageHandler>(topic);
            _services.AddTransient<TMessageHandler>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="config"></param>
        public void WithAvroSearlizerConfig(AvroSerializerConfig config)
        {
            _builder.WithAvroSearlizerConfig(config);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="config"></param>
        public void WithSchemaRegistryConfig(SchemaRegistryConfig config)
        {
            _builder.WithSchemaRegistryConfig(config);
        }

        /// <summary>
        /// Register a strategy for handling messages that are not explicitly configured with handlers
        /// </summary>
        public void WithUnconfiguredMessageHandlingStrategy<T>()
            where T : class, IUnconfiguredMessageHandlingStrategy =>
            _services.AddTransient<IUnconfiguredMessageHandlingStrategy, T>();


        /// <summary>
        /// Use the <paramref name="failureEvaluation"></paramref> evaluation method to return the desired
        /// <see cref="ConsumerFailureStrategy"/>.
        ///
        /// <para>
        ///     Failure Strategies:
        ///     <list type="table">
        ///         <listheader>
        ///             <term>Strategy</term>
        ///             <description>description</description>
        ///         </listheader>
        ///         <item>
        ///             <term><see cref="ConsumerFailureStrategy.Default"/></term>
        ///             <description><inheritdoc cref="ConsumerFailureStrategy.Default"/></description>
        ///         </item>
        ///         <item>
        ///             <term><see cref="ConsumerFailureStrategy.RestartConsumer"/></term>
        ///             <description>
        ///                 <inheritdoc cref="ConsumerFailureStrategy.RestartConsumer"/>
        ///                 Evaluation, including restart backoff strategies can be supplied here.
        ///             </description>
        ///         </item>
        ///     </list>
        /// </para>
        /// 
        /// </summary>
        /// <param name="failureEvaluation">An evaluation function that must return a
        /// <see cref="ConsumerFailureStrategy"/>.</param>
        public void WithConsumerErrorHandler(Func<Exception, Task<ConsumerFailureStrategy>> failureEvaluation)
        {
            _builder.WithConsumerErrorHandler(failureEvaluation);
        }
        private class DefaultConfigurationSource : ConfigurationSource
        {
            private readonly Microsoft.Extensions.Configuration.IConfiguration _configuration;

            public DefaultConfigurationSource(Microsoft.Extensions.Configuration.IConfiguration configuration)
            {
                _configuration = configuration;
            }

            public override string GetByKey(string key)
            {
                return _configuration[key];
            }
        }
    }
}
