using Dafda.Consuming;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dafda.Avro.Tests.TestDoubles
{
    public class MessageHandlerSpy<TMessage> : IMessageHandler<TMessage> where TMessage : class, new()
    {
        private readonly Action _onHandle;

        public MessageHandlerSpy(Action onHandle)
        {
            _onHandle = onHandle;
        }

        public Task Handle(TMessage message, MessageHandlerContext context)
        {
            _onHandle?.Invoke();
            return Task.CompletedTask;
        }
    }
}
