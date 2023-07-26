using Avro.Specific;
using Dafda.Avro.Consuming;
using Dafda.Avro.Consuming.ErrorHandlers;
using Dafda.Avro.Consuming.Interfaces;
using Dafda.Configuration;
using Dafda.Consuming.Interfaces;
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
            Func<IServiceProvider, IConsumerScopeFactory<MessageResult<TKey, TValue>>> consumerScopeFactory,
            ConsumerErrorHandler consumerErrorHandler) : base(configuration, unitOfWorkFactory, consumerErrorHandler)
        {
            AvroConsumerScopeFactory = consumerScopeFactory;
            MessageRegistration = messageRegistration;
        }
        public Func<IServiceProvider, IConsumerScopeFactory<MessageResult<TKey, TValue>>> AvroConsumerScopeFactory { get; }
        public MessageRegistration<TKey, TValue> MessageRegistration { get; }
    }
}
