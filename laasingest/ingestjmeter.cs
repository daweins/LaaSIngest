using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Azure;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Extensions.Configuration;
using Kusto.Ingest;
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
                    log.LogInformation($"Processing blob: uri= {curBlob.Uri}");
                    SharedAccessBlobPolicy blobSASPolicy = new SharedAccessBlobPolicy();
                    blobSASPolicy.SharedAccessExpiryTime = DateTime.UtcNow.AddHours(24);
                    blobSASPolicy.Permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Delete;
                    string blobSAS =  curBlob.GetSharedAccessSignature(blobSASPolicy);
                    string blobURI = curBlob.Uri.ToString() + blobSAS;
                    //log.LogInformation($"Generated Token: {blobURI}");
                    log.LogInformation($"Ingesting... {curBlob.Uri.ToString()}");
                    var ingestClient= KustoIngestFactory.CreateQueuedIngestClient(config["ingestionconn"]);
                    KustoQueuedIngestionProperties ingestProps = new KustoQueuedIngestionProperties("annandale", "jmeter");
                    ingestProps.ReportLevel = IngestionReportLevel.FailuresAndSuccesses;
                    ingestProps.FlushImmediately = true;
                    StorageSourceOptions ingestSourceOptions = new StorageSourceOptions();
                    ingestSourceOptions.DeleteSourceOnSuccess = true;
                    IKustoIngestionResult result = ingestClient.IngestFromStorageAsync(blobURI, ingestProps, ingestSourceOptions).Result;
                    foreach(var curStatus in result.GetIngestionStatusCollection())
                    {
                        log.LogInformation($"Result: status={curStatus.Status} {curStatus.ToString()} ");


                    }

                     //  $ingestProvider = [Kusto.Ingest.KustoIngestFactory]::CreateQueuedIngestClient($destADEConnStringIngest)
                    //$ingestProperties = [Kusto.Ingest.KustoQueuedIngestionProperties]::new ('annandale', 'jmeter')

                    //$ingestProperties.ReportLevel = [Kusto.Ingest.IngestionReportLevel]::FailuresAndSuccesses
                    //$ingestProperties.FlushImmediately = $true
                    //$ingestSourceOptions = [Kusto.Ingest.StorageSourceOptions]::new ()
                    //$ingestSourceOptions.DeleteSourceOnSuccess = $false
            
                    //$result = [Kusto.Ingest.ExtendedKustoIngestClient]::IngestFromSingleBlob($ingestProvider, $ingestUri, $false, $ingestProperties)


                }
            }
            catch(Exception err)
            {
                log.LogError(err.ToString());
            }
        }
    }
}
