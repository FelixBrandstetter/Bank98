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
    public static class Transactions
    {
        [FunctionName("CreateTransaction")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous,"post", Route = null)] HttpRequest req,
            [Table("Transactions", Connection = "AzureWebJobsStorage")] IAsyncCollector<Transaction> transactionsTableCollector,
            [Table("Customers", Connection = "AzureWebJobsStorage")] CloudTable customersTable,
            ILogger log)
        {
            log.LogInformation("[CreateTransaction] triggered");

            
            var requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            var input = JsonConvert.DeserializeObject<Transaction>(requestBody);

            // Check for creditor IBAN
            TableQuery<Customer> customerQueryCreditor = new()
            {
                FilterString = string.Format("IBAN eq '{0}'", input.CreditorIBAN)
            };

            var segmentCreditor = await customersTable.ExecuteQuerySegmentedAsync(customerQueryCreditor, null);
            var retrievedCustomerCreditorResult = segmentCreditor.Results;

            if (!retrievedCustomerCreditorResult.Any())
            {
                return new BadRequestObjectResult($"[GetMonthlyReport] Debitor with IBAN {input.CreditorIBAN} was not found");
            }
            
            // Check for debitor IBAN
            TableQuery<Customer> customerQueryDebitor = new()
            {
                FilterString = string.Format("IBAN eq '{0}'", input.DebitorIBAN)
            };

            var segmentDebitor = await customersTable.ExecuteQuerySegmentedAsync(customerQueryDebitor, null);
            var retrievedCustomerDebitorResult = segmentDebitor.Results;

            if (!retrievedCustomerDebitorResult.Any())
            {
                return new BadRequestObjectResult($"[GetMonthlyReport] Creditor with IBAN {input.DebitorIBAN} was not found");
            }

            var transaction = new Transaction
            {
                PartitionKey = "HTTP",
                RowKey = Guid.NewGuid().ToString(),
                ExecutionDate = input.ExecutionDate,
                Amount = input.Amount,
                Description = input.Description,
                CreditorIBAN = input.CreditorIBAN,
                DebitorIBAN = input.DebitorIBAN,
                Timestamp = DateTime.Now,
            };

            await transactionsTableCollector.AddAsync(transaction);

            return new OkObjectResult(transaction);
        }
    }

    public class Transaction : TableEntity
    {
        public DateTime ExecutionDate { get; set; }
        public double Amount { get; set; }
        public string Description { get; set; }
        public string CreditorIBAN { get; set; }
        public string DebitorIBAN { get; set; }
    }
}
