using Avro.Specific;
using Dafda.Avro.Consuming;
using Dafda.Consuming;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dafda.Avro.Tests.Builders
{
    internal class MessageRegistrationBuilder<TKey, TValue> where TValue : ISpecificRecord
    {
        private string _topic;
        private Type _handlerInstanceType;

        public MessageRegistrationBuilder()
        {
            _topic = "dummy topic";
            _handlerInstanceType = typeof(FooHandler);
        }

        public MessageRegistrationBuilder<TKey, TValue> WithTopic(string topic)
        {
            _topic = topic;
            return this;
        }

        public MessageRegistrationBuilder<TKey, TValue> WithHandlerInstanceType(Type handlerInstanceType)
        {
            _handlerInstanceType = handlerInstanceType;
            return this;
        }

        public MessageRegistration<TKey, TValue> Build()
        {
            return new MessageRegistration<TKey, TValue>(
                topic: _topic,
                handlerInstanceType: _handlerInstanceType,
                isMessageResultHandler: false
            );
        }

        #region private helper classes

        private class FooMessage
        {

        }

        private class FooHandler : IMessageHandler<FooMessage>
        {
            public Task Handle(FooMessage message, MessageHandlerContext context)
            {
                throw new NotImplementedException();
            }
        }

        #endregion
    }
}
