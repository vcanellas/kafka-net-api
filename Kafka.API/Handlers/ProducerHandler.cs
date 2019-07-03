using System;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace Kafka.API.Handlers
{
    public class ProducerHandler
    {
        private readonly string _topicName;
        private readonly ProducerConfig _config;
        private static readonly Random rand = new Random();

        public ProducerHandler(ProducerConfig config, string topicName)
        {
            _topicName = topicName;
            _config = config;
        }

        public async Task WriteMessage(string message, string key = null)
        {
            using (var producer = new ProducerBuilder<string, string>(_config).Build())
            {
                try
                {
                    var deliveryReport = await producer.ProduceAsync(_topicName, new Message<string, string>
                    {
                        Key = key,
                        Value = message
                    });
                    Console.WriteLine($"KAFKA => Delivered '{deliveryReport.Value}' to '{deliveryReport.TopicPartitionOffset}'");
                }
                catch (ProduceException<string, string> e)
                {
                    Console.WriteLine($"Failed to deliver message: {e.Message} [{e.Error.Code}]");
                }
            }
        }
    }
}