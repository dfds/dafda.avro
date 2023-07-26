using Avro.Specific;
using Dafda.Avro.Consuming;
using Dafda.Avro.Consuming.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dafda.Avro.Tests.TestDoubles
{
    internal class ConsumerScopeFactoryStub<TKey, TValue> : IConsumerScopeFactory<MessageResult<TKey, TValue>> where TValue : ISpecificRecord
    {
        private readonly IConsumerScope<MessageResult<TKey, TValue>> _result;

        public ConsumerScopeFactoryStub(IConsumerScope<MessageResult<TKey, TValue>> result)
        {
            _result = result;
        }

        public IConsumerScope<MessageResult<TKey, TValue>> CreateConsumerScope()
        {
            return _result;
        }
    }
}
