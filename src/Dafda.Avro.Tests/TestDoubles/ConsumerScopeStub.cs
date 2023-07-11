using Avro.Specific;
using Dafda.Avro.Consuming;
using Dafda.Consuming.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dafda.Avro.Tests.TestDoubles
{
    internal class ConsumerScopeStub<TKey, TValue> : IConsumerScope<MessageResult<TKey, TValue>> where TValue : ISpecificRecord
    {
        private readonly MessageResult<TKey, TValue> _result;

        public ConsumerScopeStub(MessageResult<TKey, TValue> result)
        {
            _result = result;
        }

        public Task<MessageResult<TKey, TValue>> GetNext(CancellationToken cancellationToken)
        {
            return Task.FromResult(_result);
        }

        public void Dispose()
        {

        }
    }
}
