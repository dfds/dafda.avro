﻿using Dafda.Configuration;
using Dafda.Consuming.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dafda.Avro.Consuming.ErrorHandlers
{
    internal sealed class AvroConsumerErrorHandler : IConsumerErrorHandler
    {
        public static readonly IConsumerErrorHandler Default = new AvroConsumerErrorHandler(_ => Task.FromResult(ConsumerFailureStrategy.Default));

        private readonly Func<Exception, Task<ConsumerFailureStrategy>> _eval;

        public AvroConsumerErrorHandler(Func<Exception, Task<ConsumerFailureStrategy>> eval)
        {
            _eval = eval;
        }

        public Task<ConsumerFailureStrategy> Handle(Exception exception)
        {
            return _eval(exception);
        }
    }
}
