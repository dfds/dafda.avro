using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dafda.Avro.Tests.TestDoubles
{
    public class DummyApplicationLifetime : IHostApplicationLifetime
    {
        public void StopApplication()
        {
        }

        public CancellationToken ApplicationStarted { get; }
        public CancellationToken ApplicationStopping { get; }
        public CancellationToken ApplicationStopped { get; }
    }

    public class ApplicationLifetimeSpy : IHostApplicationLifetime
    {
        public void StopApplication()
        {
            StopApplicationWasCalled = true;
        }

        public bool StopApplicationWasCalled { get; private set; }

        public CancellationToken ApplicationStarted { get; }
        public CancellationToken ApplicationStopping { get; }
        public CancellationToken ApplicationStopped { get; }
    }
}
