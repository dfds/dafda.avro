using Avro.Specific;
using Dafda.Avro.Consuming;
using Dafda.Avro.Consuming.Interfaces;
using Dafda.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dafda.Avro.Configuration.ConsumerConfigurations
{
    internal class ConsumerConfiguration<TKey, TValue> : ConsumerConfigurationBase where TValue : ISpecificRecord
    {
        public ConsumerConfiguration(IDictionary<string, string> configuration,
            MessageRegistration<TKey, TValue> messageRegistration,
            Dafda.Consuming.IHandlerUnitOfWorkFactory unitOfWorkFactory,
            Func<IServiceProvider, IAvroConsumerScopeFactory<MessageResult<TKey, TValue>>> consumerScopeFactory,
            Dafda.Consuming.Interfaces.IConsumerErrorHandler consumerErrorHandler) : base(configuration, unitOfWorkFactory, consumerErrorHandler)
        {
            AvroConsumerScopeFactory = consumerScopeFactory;
            MessageRegistration = messageRegistration;
        }
        public Func<IServiceProvider, IAvroConsumerScopeFactory<MessageResult<TKey, TValue>>> AvroConsumerScopeFactory { get; }
        public MessageRegistration<TKey, TValue> MessageRegistration { get; }
    }
}
