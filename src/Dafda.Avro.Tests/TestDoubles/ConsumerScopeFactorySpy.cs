using Avro.Specific;
using Dafda.Avro.Consuming;
using Dafda.Consuming.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dafda.Avro.Tests.TestDoubles
{
    internal class ConsumerScopeFactorySpy<TKey, TValue> : IConsumerScopeFactory<MessageResult<TKey, TValue>> where TValue : ISpecificRecord
    {
        private readonly IConsumerScope<MessageResult<TKey, TValue>> _result;

        public ConsumerScopeFactorySpy(IConsumerScope<MessageResult<TKey, TValue>> result)
        {
            _result = result;
        }

        public IConsumerScope<MessageResult<TKey, TValue>> CreateConsumerScope()
        {
            CreateConsumerScopeCalled++;
            return _result;
        }

        public int CreateConsumerScopeCalled { get; private set; }
    }
}
