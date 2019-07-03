using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.API.Domain.Models;
using Kafka.API.Handlers;
using Microsoft.Extensions.Hosting;
using Newtonsoft.Json;

namespace Kafka.API.Domain.Services
{
    public class CustomerApprovalService : BackgroundService
    {
        private readonly ConsumerConfig _consumerConfig;
        private readonly ProducerConfig _producerConfig;

        public CustomerApprovalService(ConsumerConfig consumerConfig, ProducerConfig producerConfig)
        {
            _consumerConfig = consumerConfig;
            _producerConfig = producerConfig;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            Console.WriteLine("Customer Approval Service Started");
            
          
                var consumerHelper = new ConsumerHandler(_consumerConfig, "customers_not_validated");
                var customerMessage = consumerHelper.ReadMessage(stoppingToken);
                

                //Deserilaize 
                Customer customer = JsonConvert.DeserializeObject<Customer>(customerMessage.Value);

                //TODO:: Validate Customer Approval
                Console.WriteLine($"Info: CustomerHandler => Verifying customer {customer.Name}");
                if(customer.Id == 10) throw new Exception("Teste");
                // Customer Verified
                customer.Status = Status.APPROVED;

                //Write to ReadyToShip Queue

                var producerHandler = new ProducerHandler(_producerConfig,"customers");
                await producerHandler.WriteMessage(JsonConvert.SerializeObject(customer));
            
        }
    }
}