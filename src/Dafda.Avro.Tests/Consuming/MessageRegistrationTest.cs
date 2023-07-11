using Avro;
using Avro.Specific;
using Dafda.Avro.Tests.Builders;
using Dafda.Consuming;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Dafda.Avro.Tests.Consuming
{
    public class MessageRegistrationTest
    {
        [Fact]
        public void Returns_expected_handler_instance_type()
        {
            var expectedHandlerInstanceType = typeof(FooHandler);

            var messageRegistrationStub = new MessageRegistrationBuilder<string, FooMessage>()
                .WithHandlerInstanceType(expectedHandlerInstanceType)
                .Build();

            Assert.Equal(expectedHandlerInstanceType, messageRegistrationStub.HandlerInstanceType);
        }

        [Fact]
        public void Returns_expected_message_instance_type()
        {
            var handlerInstanceTypeStub = typeof(FooHandler);
            var expectedInstanceMessageType = typeof(FooMessage);

            var messageRegistrationStub = new MessageRegistrationBuilder<string, FooMessage>()
                .WithHandlerInstanceType(handlerInstanceTypeStub)
                .Build();

            Assert.Equal(expectedInstanceMessageType, messageRegistrationStub.MessageInstanceType);
        }

        [Fact]
        public void Returns_expected_message_topic()
        {
            var expectedTopic = "foo";

            var messageRegistrationStub = new MessageRegistrationBuilder<string, FooMessage>()
                .WithHandlerInstanceType(typeof(FooHandler))
                .WithTopic(expectedTopic)
                .Build();

            Assert.Equal(expectedTopic, messageRegistrationStub.Topic);
        }

        [Fact]
        public void Throws_for_null_topic()
        {
            Assert.Throws<ArgumentException>(() =>
                new MessageRegistrationBuilder<string, FooMessage>()
                .WithHandlerInstanceType(typeof(FooHandler))
                .WithTopic(null)
                .Build());
        }

        [Fact]
        public void Throws_exception_when_handler_closes_different_message_type_than_what_its_registered_with()
        {
            var handlerInstanceTypeStub = typeof(IMessageHandler<object>);
            var messageInstanceTypeStub = typeof(FooMessage);

            var builder = new MessageRegistrationBuilder<string, FooMessage>()
                .WithHandlerInstanceType(handlerInstanceTypeStub);

            Assert.Throws<MessageRegistrationException>(() => builder.Build());
        }

        [Fact]
        public void Throws_exception_when_handler_does_NOT_implement_expected_interface()
        {
            var invalidHandlerInstanceType = typeof(object);

            var builder = new MessageRegistrationBuilder<string, FooMessage>()
                .WithHandlerInstanceType(invalidHandlerInstanceType);

            Assert.Throws<MessageRegistrationException>(() => builder.Build());
        }

        #region helper classes

        private class FooMessage : ISpecificRecord
        {
            public Schema Schema => throw new NotImplementedException();

            public object Get(int fieldPos)
            {
                throw new NotImplementedException();
            }

            public void Put(int fieldPos, object fieldValue)
            {
                throw new NotImplementedException();
            }
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
