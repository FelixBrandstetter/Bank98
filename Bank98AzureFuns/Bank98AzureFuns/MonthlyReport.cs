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
using System.Collections.Generic;

namespace Bank98AzureFuns
{
    public static class MonthlyReport
    {
        [FunctionName("GetMonthlyReport")]
        public static async Task<IActionResult> GetMonthlyReport(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", Route = null)] HttpRequest req,
            [Table("Customers", Connection = "AzureWebJobsStorage")] CloudTable customersTable,
            [Table("Transactions", Connection = "AzureWebJobsStorage")] CloudTable transactionsTable,
            ILogger log)
        {
            log.LogInformation("[GetMonthlyReport] triggered");

            string ibanInput = req.Query["iban"];
            string dateTimeInput = req.Query["month"];

            if (!DateTime.TryParse(dateTimeInput, out DateTime dateTimeParsed))
            {
                return new BadRequestObjectResult("Provide a valid date like 2022-02");
            }

            TableQuery<Customer> customerQuery = new()
            {
                FilterString = string.Format("IBAN eq '{0}'", ibanInput)
            };

            var segment = await customersTable.ExecuteQuerySegmentedAsync(customerQuery, null);
            var retrievedCustomerResult = segment.Select(x => new Customer()
            {
                Name = x.Name,
                City = x.City,
                IBAN = x.IBAN,
                Timestamp = x.Timestamp,
                PartitionKey = x.PartitionKey,
                RowKey = x.RowKey
            });

            if (!retrievedCustomerResult.Any())
            {
                return new BadRequestObjectResult($"[GetMonthlyReport] Customer with IBAN {ibanInput} was not found");
            }

            Customer retrievedCustomer = retrievedCustomerResult.First();

            log.LogInformation($"[GetMonthlyReport] Count of retrieved customers: {retrievedCustomerResult.Count()}");
            log.LogInformation("[GetMonthlyReport] Retrieved customer: {@retrievedCustomer}", retrievedCustomer);

            TableQuery<Transaction> transactionQuery = new()
            {
                FilterString = string.Format("CreditorIBAN eq '{0}' or DebitorIBAN eq '{0}'", ibanInput)
            };

            var transactionSegment = await transactionsTable.ExecuteQuerySegmentedAsync(transactionQuery, null);
            var retrievedTransactions = transactionSegment.Select(x => new Transaction()
            {
                Amount = x.Amount,
                Description = x.Description,
                CreditorIBAN = x.CreditorIBAN,
                DebitorIBAN = x.DebitorIBAN,
                ExecutionDate = x.ExecutionDate,
                Timestamp = x.Timestamp,
                PartitionKey = x.PartitionKey,
                RowKey = x.RowKey
            });

            log.LogInformation($"[GetMonthlyReport] retrieved transactions before date check count: {retrievedTransactions.Count()}");

            var selectedTransactions = retrievedTransactions.Where(x =>
                x.ExecutionDate.Month == dateTimeParsed.Month &&
                x.ExecutionDate.Year == dateTimeParsed.Year);

            MonthlyReportOutput output = new()
            {
                Customer = retrievedCustomer,
                Transactions = selectedTransactions.ToList()
            };

            return new OkObjectResult(output);
        }
    }

    public class MonthlyReportInput
    {
        public string IBAN { get; set; }
        public string Month { get; set; }
    }

    public class MonthlyReportOutput
    {
        public Customer Customer { get; set; }
        public List<Transaction> Transactions { get; set; }
    }
}
