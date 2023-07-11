using Confluent.Kafka;
using Dafda.Avro.Tests.TestDoubles;
using Dafda.Consuming.Interfaces;
using Dafda.Consuming;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using Dafda.Avro.Tests.Builders;
using Moq;
using Avro.Specific;
using Avro;
using Newtonsoft.Json.Linq;
using Dafda.Avro.Consuming;

namespace Dafda.Avro.Tests.Consuming
{
    public class AvroConsumerTests
    {
        [Fact]
        public async Task Invokes_expected_handler_when_consuming()
        {
            var handlerMock = new Mock<IMessageHandler<FooMessage>>();
            var handlerStub = handlerMock.Object;

            var messageRegistrationStub = new MessageRegistrationBuilder<string, FooMessage>()
                .WithHandlerInstanceType(handlerStub.GetType())
                .Build();

            var sut = new AvroConsumerBuilder<string, FooMessage>()
                .WithUnitOfWork(new UnitOfWorkStub(handlerStub))
                .WithMessageHandlerRegistration(messageRegistrationStub)
                .WithKey("foo")
                .WithValue(new FooMessage())
                .Build();
            

            await sut.ConsumeSingle(CancellationToken.None);

            handlerMock.Verify(x => x.Handle(It.IsAny<FooMessage>(), It.IsAny<MessageHandlerContext>()), Times.Once);
        }

        [Fact]
        public async Task Expected_order_of_handler_invocation_in_unit_of_work()
        {
            var orderOfInvocation = new LinkedList<string>();
            
            var dummyMessageResult = new MessageResultBuilder<string, FooMessage>().WithKey("foo").WithValue(new FooMessage()).Build();
            var dummyMessageRegistration = new MessageRegistrationBuilder<string, FooMessage>()
                .WithHandlerInstanceType(typeof(FooMessageHandler))
                .Build();



            var sut = new AvroConsumerBuilder<string, FooMessage>()
                .WithUnitOfWork(new UnitOfWorkSpy(
                    handlerInstance: new MessageHandlerSpy<FooMessage>(() => orderOfInvocation.AddLast("during")),
                    pre: () => orderOfInvocation.AddLast("before"),
                    post: () => orderOfInvocation.AddLast("after")
                ))
                .WithConsumerScopeFactory(new ConsumerScopeFactoryStub<string, FooMessage>(new ConsumerScopeStub<string, FooMessage>(dummyMessageResult)))
                .WithMessageHandlerRegistration(dummyMessageRegistration)
                .Build();

            await sut.ConsumeSingle(CancellationToken.None);

            Assert.Equal(new[] { "before", "during", "after" }, orderOfInvocation);
        }

        [Fact]
        public async Task Will_not_call_commit_when_auto_commit_is_enabled()
        {
            var handlerStub = Dummy.Of<IMessageHandler<FooMessage>>();

            var messageRegistrationStub = new MessageRegistrationBuilder<string, FooMessage>()
                .WithHandlerInstanceType(handlerStub.GetType())
                .Build();

            var wasCalled = false;

            var resultSpy = new MessageResultBuilder<string, FooMessage>()
                .WithKey("foo")
                .WithValue(new FooMessage())
                .WithOnCommit(() =>
                {
                    wasCalled = true;
                    return Task.CompletedTask;
                })
                .Build();
            

            var consumerScopeFactoryStub = new ConsumerScopeFactoryStub<string, FooMessage>(new ConsumerScopeStub<string, FooMessage>(resultSpy));
           
            var consumer = new AvroConsumerBuilder<string, FooMessage>()
                .WithConsumerScopeFactory(consumerScopeFactoryStub)
                .WithUnitOfWork(new UnitOfWorkStub(handlerStub))
                .WithMessageHandlerRegistration(messageRegistrationStub)
                .WithEnableAutoCommit(true)
                .Build();

            await consumer.ConsumeSingle(CancellationToken.None);

            Assert.False(wasCalled);
        }

        [Fact]
        public async Task Will_call_commit_when_auto_commit_is_disabled()
        {
            var handlerStub = Dummy.Of<IMessageHandler<FooMessage>>();

            var messageRegistrationStub = new MessageRegistrationBuilder<string, FooMessage>()
                .WithHandlerInstanceType(handlerStub.GetType())
                .Build();

            var wasCalled = false;

            var resultSpy = new MessageResultBuilder<string, FooMessage>()
                .WithKey("foo")
                .WithValue(new FooMessage())
                .WithOnCommit(() =>
                {
                    wasCalled = true;
                    return Task.CompletedTask;
                })
                .Build();
            
            var consumerScopeFactoryStub = new ConsumerScopeFactoryStub<string, FooMessage>(new ConsumerScopeStub<string, FooMessage>(resultSpy));
            
            var consumer = new AvroConsumerBuilder<string, FooMessage>()
                .WithConsumerScopeFactory(consumerScopeFactoryStub)
                .WithUnitOfWork(new UnitOfWorkStub(handlerStub))
                .WithMessageHandlerRegistration(messageRegistrationStub)
                .WithEnableAutoCommit(false)
                .Build();

            await consumer.ConsumeSingle(CancellationToken.None);

            Assert.True(wasCalled);
        }

        [Fact]
        public async Task Creates_consumer_scope_when_consuming_single_message()
        {
            var messageResultStub = new MessageResultBuilder<string, FooMessage>().WithKey("foo").WithValue(new FooMessage()).Build();
            var handlerStub = Dummy.Of<IMessageHandler<FooMessage>>();

            var messageRegistrationStub = new MessageRegistrationBuilder<string, FooMessage>()
                .WithHandlerInstanceType(handlerStub.GetType())
                .Build();

            var spy = new ConsumerScopeFactorySpy<string, FooMessage>(new ConsumerScopeStub<string, FooMessage>(messageResultStub));


            var consumer = new AvroConsumerBuilder<string, FooMessage>()
                .WithConsumerScopeFactory(spy)
                .WithUnitOfWork(new UnitOfWorkStub(handlerStub))
                .WithMessageHandlerRegistration(messageRegistrationStub)
                .Build();

            await consumer.ConsumeSingle(CancellationToken.None);


            Assert.Equal(1, spy.CreateConsumerScopeCalled);
        }

        [Fact]
        public async Task Disposes_consumer_scope_when_consuming_single_message()
        {
            var messageResultStub = new MessageResultBuilder<string, FooMessage>().WithKey("foo").WithValue(new FooMessage()).Build();
            var handlerStub = Dummy.Of<IMessageHandler<FooMessage>>();

            var messageRegistrationStub = new MessageRegistrationBuilder<string, FooMessage>()
                .WithHandlerInstanceType(handlerStub.GetType())
                .Build();

            var spy = new ConsumerScopeSpy<string, FooMessage>(messageResultStub);

            var consumer = new AvroConsumerBuilder<string, FooMessage>()
                .WithConsumerScopeFactory(new ConsumerScopeFactoryStub<string, FooMessage>(spy))
                .WithUnitOfWork(new UnitOfWorkStub(handlerStub))
                .WithMessageHandlerRegistration(messageRegistrationStub)
                .Build();

            await consumer.ConsumeSingle(CancellationToken.None);


            Assert.Equal(1, spy.Disposed);
        }

        [Fact]
        public async Task Creates_consumer_scope_when_consuming_multiple_messages()
        {
            var messageResultStub = new MessageResultBuilder<string, FooMessage>().WithKey("foo").WithValue(new FooMessage()).Build();
            var handlerStub = Dummy.Of<IMessageHandler<FooMessage>>();

            var messageRegistrationStub = new MessageRegistrationBuilder<string, FooMessage>()
                .WithHandlerInstanceType(handlerStub.GetType())
                .Build();

            using (var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
            {
                var loops = 0;

                var subscriberScopeStub = new ConsumerScopeDecoratorWithHooks<string, FooMessage>(
                    inner: new ConsumerScopeStub<string, FooMessage>(messageResultStub),
                    postHook: () =>
                    {
                        loops++;

                        if (loops == 2)
                        {
                            cancellationTokenSource.Cancel();
                        }
                    }
                );

                var spy = new ConsumerScopeFactorySpy<string, FooMessage>(subscriberScopeStub);

                
                var consumer = new AvroConsumerBuilder<string, FooMessage>()
                    .WithConsumerScopeFactory(spy)
                    .WithUnitOfWork(new UnitOfWorkStub(handlerStub))
                    .WithMessageHandlerRegistration(messageRegistrationStub)
                    .Build();

                await consumer.ConsumeAll(cancellationTokenSource.Token);

                Assert.Equal(2, loops);
                Assert.Equal(1, spy.CreateConsumerScopeCalled);
            }
        }

        [Fact]
        public async Task Disposes_consumer_scope_when_consuming_multiple_messages()
        {
            var messageResultStub = new MessageResultBuilder<string, FooMessage>().WithKey("foo").WithValue(new FooMessage()).Build();
            

            var handlerStub = Dummy.Of<IMessageHandler<FooMessage>>();

            var messageRegistrationStub = new MessageRegistrationBuilder<string, FooMessage>()
                .WithHandlerInstanceType(typeof(FooMessageHandler))
                .Build();

            using (var cancellationTokenSource = new CancellationTokenSource(TimeSpan.FromSeconds(5)))
            {
                var loops = 0;

                var spy = new ConsumerScopeSpy<string, FooMessage>(messageResultStub, () =>
                {
                    loops++;

                    if (loops == 2)
                    {
                        cancellationTokenSource.Cancel();
                    }
                });

                var consumer = new AvroConsumerBuilder<string, FooMessage>()
                    .WithConsumerScopeFactory(new ConsumerScopeFactoryStub<string, FooMessage>(spy))
                    .WithUnitOfWork(new UnitOfWorkStub(handlerStub))
                    .WithMessageHandlerRegistration(messageRegistrationStub)
                    .Build();

                await consumer.ConsumeAll(cancellationTokenSource.Token);

                Assert.Equal(2, loops);
                Assert.Equal(1, spy.Disposed);
            }
        }

        #region helper classes

        private class ConsumerScopeDecoratorWithHooks<TKey, TValue> : IConsumerScope<MessageResult<TKey, TValue>> where TValue : ISpecificRecord
        {
            private readonly IConsumerScope<MessageResult<TKey, TValue>> _inner;
            private readonly Action _preHook;
            private readonly Action _postHook;

            public ConsumerScopeDecoratorWithHooks(IConsumerScope<MessageResult<TKey, TValue>> inner, Action preHook = null, Action postHook = null)
            {
                _inner = inner;
                _preHook = preHook;
                _postHook = postHook;
            }

            public async Task<MessageResult<TKey, TValue>> GetNext(CancellationToken cancellationToken)
            {
                _preHook?.Invoke();
                var result = await _inner.GetNext(cancellationToken);
                _postHook?.Invoke();

                return result;
            }

            public void Dispose()
            {
                _inner.Dispose();
            }
        }

        public class FooMessage : ISpecificRecord
        {
            public string Value { get; set; }

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

        public class FooMessageHandler : IMessageHandler<FooMessage>
        {
            public Task Handle(FooMessage message, MessageHandlerContext context)
            {
                return Task.CompletedTask;
            }
        }

        #endregion
    }

    internal class ConsumerScopeSpy<TKey, TValue> : IConsumerScope<MessageResult<TKey, TValue>> where TValue : ISpecificRecord
    {
        private readonly Avro.Consuming.MessageResult<TKey, TValue> _messageResult;
        private readonly Action _onGetNext;

        public ConsumerScopeSpy(Avro.Consuming.MessageResult<TKey, TValue> messageResult, Action onGetNext = null)
        {
            _messageResult = messageResult;
            _onGetNext = onGetNext;
        }

        public void Dispose()
        {
            Disposed++;
        }

        Task<MessageResult<TKey, TValue>> IConsumerScope<MessageResult<TKey, TValue>>.GetNext(CancellationToken cancellationToken)
        {
            _onGetNext?.Invoke();

            return Task.FromResult(_messageResult);
        }

        public int Disposed { get; private set; }

    }
}
