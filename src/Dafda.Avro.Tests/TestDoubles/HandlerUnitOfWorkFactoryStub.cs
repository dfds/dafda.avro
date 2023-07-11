using Dafda.Consuming;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dafda.Avro.Tests.TestDoubles
{
    internal class HandlerUnitOfWorkFactoryStub : IHandlerUnitOfWorkFactory
    {
        private readonly IHandlerUnitOfWork _result;

        public HandlerUnitOfWorkFactoryStub(IHandlerUnitOfWork result)
        {
            _result = result;
        }

        public IHandlerUnitOfWork CreateForHandlerType(Type handlerType)
        {
            return _result;
        }
    }
}
