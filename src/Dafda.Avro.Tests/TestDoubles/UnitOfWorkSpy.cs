using Dafda.Consuming;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dafda.Avro.Tests.TestDoubles
{
    public class UnitOfWorkSpy : IHandlerUnitOfWork
    {
        private readonly object _handlerInstance;
        private readonly Action _pre;
        private readonly Action _post;

        public UnitOfWorkSpy(object handlerInstance, Action pre = null, Action post = null)
        {
            _handlerInstance = handlerInstance;
            _pre = pre;
            _post = post;
        }

        public async Task Run(Func<object, Task> handlingAction)
        {
            _pre?.Invoke();
            await handlingAction(_handlerInstance);
            _post?.Invoke();
        }
    }
}
