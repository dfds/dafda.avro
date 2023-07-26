using Confluent.Kafka;
using Dafda.Avro.Tests.TestDoubles;
using Dafda.Avro.Consuming.Interfaces;
using Dafda.Consuming.MessageFilters;
using Dafda.Consuming;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Confluent.Kafka.ConfigPropertyNames;
using Avro.Specific;
using Dafda.Avro.Consuming;
using Newtonsoft.Json.Linq;

namespace Dafda.Avro.Tests.Builders
{
    internal class AvroConsumerBuilder<TKey, TValue> where TValue : ISpecificRecord
    {
        private IHandlerUnitOfWorkFactory _unitOfWorkFactory;
        private IConsumerScopeFactory<MessageResult<TKey, TValue>> _consumerScopeFactory;
        private Avro.Consuming.MessageRegistration<TKey, TValue> _registration;
        private TKey _key;
        private TValue _value;

        private bool _enableAutoCommit;
        private MessageFilter _messageFilter = MessageFilter.Default;

        public AvroConsumerBuilder()
        {
            _unitOfWorkFactory = new HandlerUnitOfWorkFactoryStub(null);
            
        }

        public AvroConsumerBuilder<TKey, TValue> WithUnitOfWork(IHandlerUnitOfWork unitOfWork)
        {
            return WithUnitOfWorkFactory(new HandlerUnitOfWorkFactoryStub(unitOfWork));
        }

        public AvroConsumerBuilder<TKey, TValue> WithUnitOfWorkFactory(IHandlerUnitOfWorkFactory unitofWorkFactory)
        {
            _unitOfWorkFactory = unitofWorkFactory;
            return this;
        }

        public AvroConsumerBuilder<TKey, TValue> WithConsumerScopeFactory(IConsumerScopeFactory<MessageResult<TKey, TValue>> consumerScopeFactory)
        {
            _consumerScopeFactory = consumerScopeFactory;
            return this;
        }

        public AvroConsumerBuilder<TKey, TValue> WithMessageHandlerRegistration(Avro.Consuming.MessageRegistration<TKey, TValue> registration)
        {
            _registration = registration;
            return this;
        }

        public AvroConsumerBuilder<TKey, TValue> WithKey(TKey key)
        {
            _key = key;
            return this;
        }

        public AvroConsumerBuilder<TKey, TValue> WithValue(TValue value)
        {
            _value = value;
            return this;
        }

        public AvroConsumerBuilder<TKey, TValue> WithEnableAutoCommit(bool enableAutoCommit)
        {
            _enableAutoCommit = enableAutoCommit;
            return this;
        }

        public AvroConsumer<TKey, TValue> Build()
        {
            if(_consumerScopeFactory == null)
            {
                if(_key == null || _value == null)
                    throw new Exception("Missing key or value");

                var messageResult = new MessageResultBuilder<TKey, TValue>().WithKey(_key).WithValue(_value).Build();
                _consumerScopeFactory = new ConsumerScopeFactoryStub<TKey, TValue>(new ConsumerScopeStub<TKey, TValue>(messageResult));
            }


            return new AvroConsumer<TKey, TValue>(
            _registration,
            _unitOfWorkFactory,
            _consumerScopeFactory,
            _enableAutoCommit
            );
        }
            
            
    }
}
