using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Avro.Specific;
using Dafda.Avro.Consuming.Interfaces;
using Confluent.Kafka;
using Chr.Avro.Confluent;

namespace Dafda.Avro.Consuming.Factories
{
    internal class AvroBasedConsumerScopeFactory<TKey, TValue> : IAvroConsumerScopeFactory<MessageResult<TKey, TValue>> where TValue : ISpecificRecord
    {
        internal readonly ILoggerFactory _loggerFactory;
        internal readonly IEnumerable<KeyValuePair<string, string>> _configuration;
        internal readonly string _topic;
        internal readonly bool _readFromBeginning;
        internal readonly SchemaRegistryConfig _schemaRegistryConfig;
        internal readonly AvroSerializerConfig _avroSerializerConfig;

        private static IConsumer<TKey, TValue> _consumer = null;

        public AvroBasedConsumerScopeFactory(ILoggerFactory loggerFactory, IEnumerable<KeyValuePair<string, string>> configuration, string topic, bool readFromBeginning, SchemaRegistryConfig schemaRegistryConfig, AvroSerializerConfig avroSerializerConfig)
        {
            _loggerFactory = loggerFactory;
            _configuration = configuration;
            _topic = topic;
            _readFromBeginning = readFromBeginning;
            _schemaRegistryConfig = schemaRegistryConfig;
            _avroSerializerConfig = avroSerializerConfig;
        }

        public IConsumerScope<MessageResult<TKey, TValue>> CreateConsumerScope()
        {
            if(_consumer == null)
            {
                var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);

                var builder = new ConsumerBuilder<TKey, TValue>(_configuration);

                var consumerBuilder = new ConsumerBuilder<TKey, TValue>(_configuration)
                                        .SetAvroKeyDeserializer(schemaRegistry)
                                        .SetAvroValueDeserializer(schemaRegistry);

                if (_readFromBeginning)
                    consumerBuilder.SetPartitionsAssignedHandler((cons, topicPartitions) => { return topicPartitions.Select(tp => new TopicPartitionOffset(tp, Offset.Beginning)); });

                _consumer = consumerBuilder.Build();
                _consumer.Subscribe(_topic);
            }
           
            return new AvroConsumerScope<TKey, TValue>(_loggerFactory, _consumer);
        }
    }
}
