using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Cosmos.Table;
using System.Linq;

namespace Bank98AzureFuns
{
    public static class Customers
    {
        [FunctionName("CreateCustomer")]
        public static async Task<IActionResult> CreateCustomer(
            [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = null)] HttpRequest req,
            [Table("Customers", Connection = "AzureWebJobsStorage")] IAsyncCollector<Customer> customersTableCollector,
            ILogger log)
        {
            log.LogInformation("[CreateCustomer] triggered");

            var requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var input = JsonConvert.DeserializeObject<Customer>(requestBody);

            var customer = new Customer
            {
                PartitionKey = "HTTP",
                RowKey = Guid.NewGuid().ToString(),
                Name = input.Name,
                City = input.City,
                IBAN = input.IBAN,
                Timestamp = DateTime.Now,
            };
            
            await customersTableCollector.AddAsync(customer);

            return new OkObjectResult(customer);
        }

        [FunctionName("GetCustomers")]
        public static async Task<IActionResult> GetCustomers(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = null)] HttpRequest req,
            [Table("Customers", Connection = "AzureWebJobsStorage")] CloudTable cloudTable,
            ILogger log)
        {
            log.LogInformation("[GetCustomers] triggered");
            
            TableQuery<Customer> query = new();
            var segment = await cloudTable.ExecuteQuerySegmentedAsync(query, null);
            var data = segment.Select(x => new Customer() 
            { 
                Name = x.Name, 
                City = x.City, 
                IBAN = x.IBAN, 
                Timestamp = x.Timestamp, 
                PartitionKey = x.PartitionKey,
                RowKey = x.RowKey});

            return new OkObjectResult(data);
        }
    }

    public class Customer : TableEntity
    {
        public string Name { get; set; }
        public string City { get; set; }
        public string IBAN { get; set; }
    }
}
