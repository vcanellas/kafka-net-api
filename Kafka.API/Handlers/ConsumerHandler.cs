using System;
using System.Threading;
using Confluent.Kafka;

namespace Kafka.API.Handlers
{
    public class ConsumerHandler
    {
        private string _topicName;
        private ConsumerConfig _consumerConfig;
        private IConsumer<Ignore, string> _consumer;
        private static readonly Random rand = new Random();

        public ConsumerHandler(ConsumerConfig config, string topicName)
        {
            _topicName = topicName;
            _consumerConfig = config;
            
        }

        public ConsumeResult<Ignore, string> ReadMessage(CancellationToken cancellationToken)
        {
            try
            {
                using (_consumer = new ConsumerBuilder<Ignore, string>(_consumerConfig)
                    .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                    .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                    .SetPartitionsAssignedHandler((c, partitions) =>
                    {
                        Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}]");
                        // possibly manually specify start offsets or override the partition assignment provided by
                        // the consumer group by returning a list of topic/partition/offsets to assign to, e.g.:
                        // 
                        // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                    })
                    .SetPartitionsRevokedHandler((c, partitions) => { Console.WriteLine($"Revoking assignment: [{string.Join(", ", partitions)}]"); })
                    .Build())
                {
                    _consumer.Subscribe(_topicName);
                    ConsumeResult<Ignore, string> message = null;
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        message = _consumer.Consume(cancellationToken);

                        if (!message.IsPartitionEOF) return message;
                        
                        Console.WriteLine($"Reached end of topic {message.Topic}, partition {message.Partition}, offset {message.Offset}.");
                    }

                    return message;
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Closing consumer.");
                _consumer.Close();
                return null;
            }
        }
    }
}