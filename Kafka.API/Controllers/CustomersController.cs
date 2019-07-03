using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;
using Kafka.API.Domain.Models;
using Kafka.API.Handlers;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace Kafka.API.Controllers
{
    [Route("api/customers")]
    [ApiController]
    public class CustomersController : ControllerBase
    {
        private readonly ProducerConfig _producerConfig;

        public CustomersController(ProducerConfig producerConfig)
        {
            _producerConfig = producerConfig;
        }

        // POST api/customers/accept
        [HttpPost("accept")]
        public async Task<ActionResult> AcceptCustomerAsync([FromBody]Customer customer)
        {
            if (!ModelState.IsValid)
            {
                return BadRequest(ModelState);
            }

            //Serialize 
            string serializedCustomer = JsonConvert.SerializeObject(customer);

            Console.WriteLine("========");
            Console.WriteLine("Info: CustomersController => Post => Received new customer to approve:");
            Console.WriteLine(serializedCustomer);
            Console.WriteLine("=========");

            var producer = new ProducerHandler(_producerConfig,"customers_not_validated");
            await producer.WriteMessage(serializedCustomer);

            return Created("CreationId", "Processing customer approval");
        }
        
    }
}