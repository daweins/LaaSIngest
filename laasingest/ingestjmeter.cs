using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Azure;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Configuration;
using Microsoft.Azure.Storage.Auth;

namespace laasingest
{
    public static class ingestjmeter
    {
        [FunctionName("IngestJmeter")]
        public static void Run([TimerTrigger("0 * * * * *")]TimerInfo myTimer, ILogger log, ExecutionContext context)
        {
            try
            {
               var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build(); 

                log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
                StorageCredentials creds = new StorageCredentials(config["storname"], config["storkey"]);
                CloudStorageAccount csa = new CloudStorageAccount(creds,true);
                CloudBlobClient cbc = csa.CreateCloudBlobClient();
                CloudBlobContainer container = cbc.GetContainerReference(config["storcontainer"]);
                log.LogInformation($"Got the container: uri= {container.Uri}  name= {container.Name} ");
                foreach (CloudBlockBlob curBlob in container.ListBlobs())
                {
                    KustoHelper.uploadBlobToKusto(curBlob, config["ingestionconn"], "annandale", "jmeterIngest",log);
                    

                }
            }
            catch(Exception err)
            {
                log.LogError(err.ToString());
                throw new Exception(err.ToString());
            }
        }
    }
}
