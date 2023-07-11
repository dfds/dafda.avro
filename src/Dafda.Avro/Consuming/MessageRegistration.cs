using Avro.Specific;
using Dafda.Consuming;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dafda.Avro.Consuming
{
    /// <summary>
    /// Create a message registration with the given properties,
    /// will throw if the handler doesn't match the message
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TValue"></typeparam>
    public sealed class MessageRegistration<TKey, TValue> where TValue : ISpecificRecord
    {
        /// <summary>
        /// CTOR
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="handlerInstanceType"></param>
        /// <param name="isMessageResultHandler"></param>
        public MessageRegistration(string topic, Type handlerInstanceType, bool isMessageResultHandler)
        {
            IsMessageResultHandler = isMessageResultHandler;
            EnsureProperHandlerType(handlerInstanceType, typeof(TValue), isMessageResultHandler);
            Topic = EnsureValidTopicName(topic);
            HandlerInstanceType = handlerInstanceType;
        }
        /// <summary>The type of the message handler</summary>
        public Type HandlerInstanceType { get; }

        /// <summary>The type of the message</summary>
        public Type MessageInstanceType { get; } = typeof(TValue);

        /// <summary>The type of the key</summary>
        public Type KeyInstanceType { get; } = typeof(TKey);
        /// <summary>The name of the kafka topic</summary>
        public string Topic { get; }

        /// <summary>
        /// Indicates if its a handler for a MessageResult or just a Message
        /// </summary>
        public bool IsMessageResultHandler { get; }

        private static void EnsureProperHandlerType(Type handlerInstanceType, Type messageInstanceType, bool isMessageResultHandler)
        {
            var expectedHandlerInstanceBaseType = typeof(IMessageHandler<>).MakeGenericType(messageInstanceType); //TODO come back to this

            if (isMessageResultHandler)
                expectedHandlerInstanceBaseType = typeof(IMessageHandler<>).MakeGenericType(typeof(MessageResult<TKey, TValue>)); //TODO come back to this

            if (expectedHandlerInstanceBaseType.IsAssignableFrom(handlerInstanceType))
                return;

            var openGenericInterfaceName = typeof(IMessageHandler<>).Name;
            var expectedInterface = $"{openGenericInterfaceName.Substring(0, openGenericInterfaceName.Length - 2)}<{messageInstanceType.FullName}>";

            throw new MessageRegistrationException($"Error! Message handler type \"{handlerInstanceType.FullName}\" does not implement expected interface \"{expectedInterface}\". It's expected when registered together with a message instance type of \"{messageInstanceType.FullName}\".");
        }

        private static string EnsureValidTopicName(string topicName)
        {
            // Passing a null topic, will cause Confluent to throw an AccessViolationException, which cannot be caught by the service, resulting in a hard crash without logs.
            if (topicName == null)
            {
                throw new ArgumentException(nameof(topicName), "Topic must have a value");
            }

            return topicName;
        }
    }
}
