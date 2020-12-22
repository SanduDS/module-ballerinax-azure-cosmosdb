The Cosmos DB connector allows you to connect to a Azure Cosmos DB resource from Ballerina and perform various operations such as `find`, `create`, `read`, `update`, and `delete` operations of `Databases`, `Containers`, `User Defined Functions`, `Tiggers`, `Stored Procedures`, `Users`, `Permissions` and `Offers`.

## Compatibility

|                       |      Version       |
| :-------------------: | :----------------: |
|  Ballerina Language   | Swan Lake Preview7 |
| Cosmos DB API Version |     2018-12-31     |

## CosmosDB Clients

There is only one client provided by Ballerina to interact with CosmosDB.

1. **azure_cosmosdb:Client** - This connects to the running CosmosDB resource and perform different actions

   ```ballerina
   AzureCosmosConfiguration azureConfig = {
   baseUrl : <"BASE_URL">,
   keyOrResourceToken : <"KEY_OR_RESOURCE_TOKEN">,
   host : <"HOST">,
   tokenType : <"TOKEN_TYPE">,
   tokenVersion : <"TOKEN_VERSION">
   };
   Client azureClient = check new (azureConfig);
   ```

## Sample

First, import the `ballerinax/azure_cosmosdb` module into the Ballerina project.

```ballerina
import ballerina/log;
import ballerinax/azure_cosmosdb;

public function main() {

    AzureCosmosConfiguration config = {
    baseUrl : "https://cosmosconnector.documents.azure.com:443",
    keyOrResourceToken : "mytokenABCD==",
    host : "cosmosconnector.documents.azure.com:443",
    tokenType : "master",
    tokenVersion : "1.0"
    };

    azure_cosmosdb:Client azureCosmosClient = new(config);

    Database database1 = azureCosmosClient->createDatabase("mydatabase");

    PartitionKey partitionKey = {
        paths: ["/AccountNumber"],
        kind :"Hash",
        'version: 2
    };
    Container container1 = azureCosmosClient->createContainer(database1.id, "mycontainer", partitionKey);

    Document document1 = { id: "documentid1", documentBody :{ "LastName": "Sheldon", "AccountNumber": 1234 }, partitionKey : [1234] };
    Document document2 = { id: "documentid2", documentBody :{ "LastName": "West", "AccountNumber": 7805 }, partitionKey : [7805] };
    Document document3 = { id: "documentid3", documentBody :{ "LastName": "Moore", "AccountNumber": 5678 }, partitionKey : [5678] };
    Document document4 = { id: "documentid4", documentBody :{ "LastName": "Hope", "AccountNumber": 2343 }, partitionKey : [2343] };

    log:printInfo("------------------ Inserting Documents -------------------");
    var output1 = azureCosmosClient->createDocument(database1.id, container1.id, document1);
    var output2 = azureCosmosClient->createDocument(database1.id, container1.id,, document2);
    var output3 = azureCosmosClient->createDocument(database1.id, container1.id,, document3);
    var output4 = azureCosmosClient->createDocument(database1.id, container1.id,, document4);

    log:printInfo("------------------ List Documents -------------------");
    stream<Document> documentList = azureCosmosClient->getDocumentList(database1.id, container1.id)

    log:printInfo("------------------ Get One Document -------------------");
    Document document = azureCosmosClient->getDocument(database1.id, container1.id, document1.id, [1234])

    log:printInfo("------------------ Query Documents -------------------");
    Query sqlQuery = {
        query: string `SELECT * FROM ${container1.id.toString()} f WHERE f.Address.City = 'Seattle'`,
        parameters: []
    };
    var resultStream = AzureCosmosClient->queryDocuments(database1.id, container1.id, [1234], sqlQuery);
    error? e = result.forEach(function (json document){
                    log:printInfo(document);
                });    

    log:printInfo("------------------ Delete Document -------------------");
    var result = AzureCosmosClient->deleteDocument(database1.id, container1.id, document.id, [1234]);

}
```
