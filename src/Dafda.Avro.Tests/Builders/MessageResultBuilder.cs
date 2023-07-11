using Avro.Specific;
using Dafda.Avro.Consuming;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dafda.Avro.Tests.Builders
{
    internal class MessageResultBuilder<TKey, TValue> where TValue : ISpecificRecord
    {
        private Func<Task> _onCommit;
        private TKey _key;
        private TValue _value;
        private IEnumerable<KeyValuePair<string, byte[]>> _headers;
        private MessageMetadata _messageMetadata;

        public MessageResultBuilder()
        {
            _onCommit = () => Task.CompletedTask;
            _headers = new List<KeyValuePair<string, byte[]>>();
            _messageMetadata = new MessageMetadata("Topic", 1, new Confluent.Kafka.Timestamp(), 1);
        }

        public MessageResultBuilder<TKey, TValue> WithKey(TKey key)
        {
            _key = key;
            return this;
        }

        public MessageResultBuilder<TKey, TValue> WithValue(TValue value)
        {
            _value = value;
            return this;
        }

        public MessageResultBuilder<TKey, TValue> WithOnCommit(Func<Task> onCommit)
        {
            _onCommit = onCommit;
            return this;
        }
        public MessageResultBuilder<TKey, TValue> WithMessageMetaData(MessageMetadata messageMetadata)
        {
            _messageMetadata = messageMetadata;
            return this;
        }

        public MessageResult<TKey, TValue> Build()
        {
            return new MessageResult<TKey, TValue>(_key, _value, _headers, _messageMetadata, _onCommit);
        }
    }
}
