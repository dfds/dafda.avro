using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dafda.Avro.Consuming.Interfaces
{
    public interface IConsumerScopeFactory<TResult>
    {
        /// <summary>
        /// Creates consumer scope with result type defined in TResult
        /// </summary>
        IConsumerScope<TResult> CreateConsumerScope();
    }
}
