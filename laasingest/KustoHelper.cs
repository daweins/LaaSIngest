using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage;
using System.IO;
using Microsoft.Azure.Storage.Auth;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Kusto.Ingest;

namespace laasingest
{
    class KustoHelper
    {

        static public void uploadBlobToKusto(CloudBlockBlob curBlob ,  string ingestConn, string dbName, string tableName, ILogger log)
        {
            log.LogInformation($"Processing blob: uri= {curBlob.Uri}");
            // Check for 0 blob size and get rid of them
            if ((curBlob.Properties.Length == 0) && (curBlob.Properties.LastModified < DateTime.UtcNow.AddHours(-2)))
            {
                log.LogInformation($"Deleting a zero length blob {curBlob.Name} with date {curBlob.Properties.LastModified}");
                curBlob.DeleteAsync();
                return;
            }
            if((curBlob.Properties.Length ==  0))
            {
                log.LogInformation($"Ignoring a zero length blob {curBlob.Name} with date {curBlob.Properties.LastModified}");
                return;

            }
            SharedAccessBlobPolicy blobSASPolicy = new SharedAccessBlobPolicy();
            blobSASPolicy.SharedAccessExpiryTime = DateTime.UtcNow.AddHours(24);
            blobSASPolicy.Permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Delete;
            string blobSAS = curBlob.GetSharedAccessSignature(blobSASPolicy);
            string blobURI = curBlob.Uri.ToString() + blobSAS;
            //log.LogInformation($"Generated Token: {blobURI}");
            log.LogInformation($"Ingesting... {curBlob.Uri.ToString()}");
            var ingestClient = KustoIngestFactory.CreateQueuedIngestClient(ingestConn);
            KustoQueuedIngestionProperties ingestProps = new KustoQueuedIngestionProperties(dbName, tableName);
            ingestProps.ReportLevel = IngestionReportLevel.FailuresAndSuccesses;
            ingestProps.FlushImmediately = true;
            StorageSourceOptions ingestSourceOptions = new StorageSourceOptions();
            ingestSourceOptions.DeleteSourceOnSuccess = true;
            IKustoIngestionResult result = ingestClient.IngestFromStorageAsync(blobURI, ingestProps, ingestSourceOptions).Result;
            foreach (var curStatus in result.GetIngestionStatusCollection())
            {
                log.LogInformation($"Result: status={curStatus.Status} Details: {curStatus.Details} ");


            }
        }
        static public void dedupeKusto()
        {


        }
    }
}
