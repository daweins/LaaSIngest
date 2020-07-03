using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Configuration;
using Microsoft.Azure.Storage.Auth;
using Kusto.Data.Net.Client;
using Kusto.Data.Common;
using Kusto.Ingest;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace laasingest
{
    public static class kustoPipe
    {
        [FunctionName("kustoPipe")]
        public static void Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, ILogger log, Microsoft.Azure.WebJobs.ExecutionContext context)
        {
            bool allGood = true;
            var config = new ConfigurationBuilder()
              .SetBasePath(context.FunctionAppDirectory)
              .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
              .AddEnvironmentVariables()
              .Build();
            // Execute Kusto queries, then save the output and ingest that output into Kusto
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            // Get all KQL from Blob Store
            StorageCredentials creds = new StorageCredentials(config["storname"], config["storkey"]);
            CloudStorageAccount csa = new CloudStorageAccount(creds, true);
            CloudBlobClient cbc = csa.CreateCloudBlobClient();

            CloudBlobContainer container = cbc.GetContainerReference(config["storcontainerkql"]);

            CloudBlobContainer tempContainer = cbc.GetContainerReference(config["storcontainerkp"]);
            foreach (CloudBlockBlob curBlob in container.ListBlobs())
            {
                if (curBlob.Name.EndsWith(".kql"))
                {
                    try
                    {
                        // Process this one
                        log.LogInformation($"Processing {curBlob.Name}");
                        string query = curBlob.DownloadText();

                        // Get the destination table name
                        Regex destRegex = new Regex("//.*Destination=\\s*(?<destTable>\\w+)");
                        Match m = destRegex.Match(query);
                        if (m.Success)
                        {
                            string destTable = m.Result("${destTable}");
                            log.LogInformation($"Got Destination table: {destTable}");
                            var queryProvider = KustoClientFactory.CreateCslQueryProvider(config["dataConn"]);
                            ClientRequestProperties props = new ClientRequestProperties();

                            props.ClientRequestId = "AzFunc" + Guid.NewGuid();
                            log.LogInformation($"Client Request ID: {props.ClientRequestId}");
                            props.SetOption(ClientRequestProperties.OptionServerTimeout, TimeSpan.FromSeconds(300));
                            log.LogInformation($"Executing query: {query}");

                            var result = queryProvider.ExecuteQuery(query, props);

                            // Write output to a blob
                            string newBlobName = $"{curBlob.Name.Replace(".kql", "")}-{DateTime.UtcNow.ToString("yyyyMMdd-hhmmss-fff")}.csv";


                            CloudBlockBlob newBlob = tempContainer.GetBlockBlobReference(newBlobName);


                            using (StreamWriter sw = new StreamWriter(newBlob.OpenWrite()))
                            {
                                using (CsvHelper.CsvWriter csvWriter = new CsvHelper.CsvWriter(sw))
                                {
                                    while (result.Read())
                                    {
                                        for (int curCol = 0; curCol < result.FieldCount; curCol++)
                                        {
                                            try
                                            {
                                                if(results[curCol] != null)
                                                {
                                                    resultString = results[curCol].ToString();  
                                                    log.LogInformation($"Writing fieldNum {curCol}, with value {resultString}");
                                                    csvWriter.WriteField(resultString);
                                                }
                                                else
                                                {
                                                    log.LogInformation($"FieldNum {curCol} is null - skipping");    
                                                }
                                                
                                            }
                                            catch(Exception err)
                                            {
                                                log.LogError($"Error with fieldNum {curCol}: {err.ToString()}, skipping");   
                                            }
                                        }
                                        csvWriter.NextRecord();
                                    }
                                }
                            }


                            KustoHelper.uploadBlobToKusto(newBlob, config["ingestionconn"], "annandale", destTable, log);

                            // WAit for ingestion
                            log.LogInformation("Sleeping for 15 seconds for ingestion");
                            Thread.Sleep(15000); ;
                            
                        }
                        else
                        {
                            log.LogError("Unable to parse destination table");
                            allGood = false;
                        }


                    }
                    catch (Exception queryErr)
                    {
                        log.LogError(queryErr.ToString());
                        allGood = false;
                    }
                    log.LogInformation("Complete");
                    if (!allGood)
                    {
                        throw new Exception("Had at least one issue");
                    }

                }
            }




        }
    }
}
