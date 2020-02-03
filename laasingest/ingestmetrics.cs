using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Kusto.Data.Net.Client;
using Kusto.Data.Common;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using System.Net.Http;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Text;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage;
using System.IO;
using Microsoft.Azure.Storage.Auth;

namespace laasingest
{
    public static class ingestmetrics
    {
        [FunctionName("ingestmetrics")]
        public static void Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, ILogger log, ExecutionContext context)
        {
            var config = new ConfigurationBuilder()
               .SetBasePath(context.FunctionAppDirectory)
               .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
               .AddEnvironmentVariables()
               .Build();

            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            string latestMetricQuery = config["getLatestMetricQuery"].Replace("{MY_SUB}", config["mySub"]);
            var queryProvider = KustoClientFactory.CreateCslQueryProvider(config["dataConn"]);
            ClientRequestProperties props = new ClientRequestProperties();

            props.ClientRequestId = "AzFunc" + Guid.NewGuid();
            log.LogInformation($"Client Request ID: {props.ClientRequestId}");
            props.SetOption(ClientRequestProperties.OptionServerTimeout, TimeSpan.FromSeconds(30));
            log.LogInformation("Executing latest metric query");
            DateTime mostRecentMetric = DateTime.MinValue;
            try
            {
                var result = queryProvider.ExecuteQuery(latestMetricQuery, props);
                result.Read();
                mostRecentMetric = result.GetDateTime(0);
            }
            catch (Exception err)
            {
                log.LogError(err.ToString());
            }
            log.LogInformation($"Most recent azure metric is: {mostRecentMetric}");
            string authURL = $"https://login.microsoftonline.com/{config["myTenant"]}";
            ClientCredential myCred = new ClientCredential(config["serviceClientId"], config["servicePwd"]);
            AuthenticationContext myAuthContext = new AuthenticationContext(authURL);
            var authToken = myAuthContext.AcquireTokenAsync("https://management.core.windows.net/", myCred).Result.AccessToken;
            string loginURL = $"https://login.microsoftonline.com/{config["myTenant"]}/oauth2/token";
            string resource = "https://api.loganalytics.io";
            using (var client = new HttpClient())
            {
                try
                {
                    client.DefaultRequestHeaders.Accept.Clear();
                    client.DefaultRequestHeaders.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));
                    Dictionary<string, string> requestData = new Dictionary<string, string>();
                    requestData["grant_type"] = "client_credentials";
                    requestData["client_id"] = config["serviceClientId"];
                    requestData["client_secret"] = config["servicePwd"];
                    requestData["resource"] = resource;
                    FormUrlEncodedContent requestBody = new FormUrlEncodedContent(requestData);
                    string response = client.PostAsync(loginURL, requestBody).Result.Content.ReadAsStringAsync().Result;
                    string token = JsonConvert.DeserializeObject<dynamic>(response).access_token.Value;

                    string logQueryString = $"AzureMetrics | where MetricName == 'HealthyHostCount' or MetricName == 'UnhealthyHostCount' or MetricName == 'TotalRequests' or MetricName == 'ResponseStatus' or MetricName == 'AvgRequestCountPerHealthyHost' | project SubscriptionId, ResourceId, Resource, MetricName, Average, Minimum,Maximum,Total, UnitName,ResourceGroup,TimeGenerated,TimeGrain |  where isnotnull(TimeGenerated) and TimeGenerated > datetime({mostRecentMetric}) | order by TimeGenerated asc | limit 10000";
                    string logQueryJson = JsonConvert.SerializeObject(new
                    {
                        query = logQueryString,

                    });

                    client.DefaultRequestHeaders.Accept.Clear();
                    client.DefaultRequestHeaders.Accept.Add(new System.Net.Http.Headers.MediaTypeWithQualityHeaderValue("application/json"));
                    client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
                    var postContent = new StringContent(logQueryJson, System.Text.Encoding.UTF8, "application/json");
                    string logUri = $"https://api.loganalytics.io/v1/workspaces/{config["logAnalyticsWorkspace"]}/query";
                    var response2 = client.PostAsync(logUri, postContent).Result;
                    var response2Content = response2.Content.ReadAsStringAsync().Result;
                    LogAnalyticsResult laResult = JsonConvert.DeserializeObject<LogAnalyticsResult>(response2Content);
                    StringBuilder csvOut = new StringBuilder();
                    for(int curRowNum = 0; curRowNum < laResult.tables[0].rows.Length; curRowNum++)
                    {
                        if(curRowNum % 1000 == 0 )
                        {
                            log.LogInformation($"Parsing Row {curRowNum}/{laResult.tables[0].rows.Length}");
                        }
                        object[] curRow = laResult.tables[0].rows[curRowNum];
                        for(int curObjNum =0; curObjNum < curRow.Length; curObjNum++)
                        {
                            if(curObjNum > 0)
                            {
                                csvOut.Append(',');
                            }
                            try
                            {
                                if (curRow[curObjNum] != null)
                                {
                                    csvOut.Append(curRow[curObjNum].ToString());
                                }
                            }
                            catch(Exception outputError)
                            {
                                log.LogError($"Error writing row {curRowNum} column {curObjNum}");
                            }
                        }
                        csvOut.Append('\n');
                    }
                    log.LogInformation("Generated the CSV: writing to Azure Blob");
                    // Write to Azure Blob so it can be ingested
                    StorageCredentials creds = new StorageCredentials(config["storname"], config["storkey"]);
                    CloudStorageAccount csa = new CloudStorageAccount(creds, true);
                    CloudBlobClient cbc = csa.CreateCloudBlobClient();

                    CloudBlobContainer container = cbc.GetContainerReference(config["storcontainerla"]);
                    container.CreateIfNotExists();
                    log.LogInformation($"Got the container: uri= {container.Uri}  name= {container.Name} ");


                    string blobName = $"loganalyticsmetrics{DateTime.UtcNow.ToString("yyyyMMdd-hhmmss-ffff")}.csv";
                    CloudBlockBlob curBlob = container.GetBlockBlobReference(blobName);
                    
                    StringReader sr = new StringReader(csvOut.ToString());
                    curBlob.UploadText(csvOut.ToString());
                    log.LogInformation($"Created blob: {blobName}");

                    KustoHelper.uploadBlobToKusto(curBlob, config["ingestionconn"], "annandale", "azureMetrics", log);





                }
                catch (Exception err)
                {
                    log.LogError(err.ToString());
                    throw new Exception(err.ToString());
                }
            }
        }
    }
}
