using Dafda.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dafda.Avro.Tests.TestDoubles
{
    public class ConfigurationSourceStub : ConfigurationSource
    {
        private readonly IDictionary<string, string> _configuration;

        public ConfigurationSourceStub(params (string key, string value)[] configuration)
        {
            _configuration = configuration.ToDictionary(x => x.key, x => x.value);
        }

        public override string GetByKey(string key)
        {
            _configuration.TryGetValue(key, out var value);
            return value;
        }
    }
}
