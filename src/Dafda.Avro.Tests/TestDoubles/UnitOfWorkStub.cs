using Dafda.Consuming;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dafda.Avro.Tests.TestDoubles
{
    public class UnitOfWorkStub : IHandlerUnitOfWork
    {
        private readonly object _handlerInstance;

        public UnitOfWorkStub(object handlerInstance)
        {
            _handlerInstance = handlerInstance;
        }

        public async Task Run(Func<object, Task> handlingAction)
        {
            await handlingAction(_handlerInstance);
        }
    }
}
