import ballerina/http;

public client class Database  {
    
    private string baseUrl;
    private string keyOrResourceToken;
    private string host;
    private string keyType;
    private string tokenVersion;
    private AzureCosmosConfiguration azureConfig;
    private string databaseId;

    public http:Client azureCosmosClient;

    function init(string databaseId, AzureCosmosConfiguration azureConfig) {
        self.azureConfig = azureConfig;
        self.databaseId = databaseId;
        self.baseUrl = azureConfig.baseUrl;
        self.keyOrResourceToken = azureConfig.keyOrResourceToken;
        self.host = azureConfig.host;
        self.keyType = azureConfig.tokenType;
        self.tokenVersion = azureConfig.tokenVersion;
        http:ClientConfiguration httpClientConfig = {secureSocket: azureConfig.secureSocketConfig};
        self.azureCosmosClient = new (self.baseUrl, httpClientConfig);
    }

    # Create a collection inside a database
    # + containerId - object of type ResourceProperties
    # + partitionKey - required object of type PartitionKey
    # + indexingPolicy - optional object of type IndexingPolicy
    # + throughputProperties - Optional throughput parameter which will set 'x-ms-offer-throughput' header 
    # + return - If successful, returns Container. Else returns error.  
    public remote function createContainer(string containerId, PartitionKey partitionKey, 
    IndexingPolicy? indexingPolicy = (), ThroughputProperties? throughputProperties = ()) returns @tainted ContainerResponse|error {
        http:Request request = new;
        string requestPath = prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS]);
        HeaderParameters header = mapParametersToHeaderType(POST, requestPath);
        json jsonPayload = {
            "id": containerId, 
            "partitionKey": {
                paths: <json>partitionKey.paths.cloneWithType(json), 
                kind : partitionKey.kind, 
                Version: partitionKey?.keyVersion
            }
        };
        if(indexingPolicy != ()) {
            jsonPayload = check jsonPayload.mergeJson({"indexingPolicy": <json>indexingPolicy.cloneWithType(json)});
        }
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        request = check setThroughputOrAutopilotHeader(request, throughputProperties);
        request.setJsonPayload(<@untainted>jsonPayload);
        var response = self.azureCosmosClient->post(requestPath, request);
        [json, Headers] jsonreponse = check mapResponseToTuple(response);
        return mapJsonToContainerType(jsonreponse);
    }

    # Create a database inside a resource
    # + containerId -  object of type ResourceProperties
    # + partitionKey - 
    # + indexingPolicy -
    # + throughputProperties - Optional throughput parameter which will set 'x-ms-offer-throughput' header 
    # + return - If successful, returns Database. Else returns error.  
    public remote function createContainerIfNotExist(string containerId, PartitionKey partitionKey, 
    IndexingPolicy? indexingPolicy = (), ThroughputProperties? throughputProperties = ()) returns @tainted ContainerResponse?|error {
        var result = self->getContainer(containerId);
        if result is error {
            string status = result.detail()["status"].toString();
            if(status == STATUS_NOT_FOUND_STRING){
                return self->createContainer(containerId, partitionKey, indexingPolicy, throughputProperties);
            } else {
                return prepareError(string `External azure error ${status}`);
            }
        }
        return ();
    }

    # List all collections inside a database
    # + maxItemCount - 
    # + return - If successful, returns ContainerList. Else returns error.  
    public remote function getAllContainers(int? maxItemCount = ()) returns @tainted stream<ContainerResponse>|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS]);
        HeaderParameters header = mapParametersToHeaderType(GET, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        if(maxItemCount is int){
            request.setHeader(MAX_ITEM_COUNT_HEADER, maxItemCount.toString()); 
        }
        stream<ContainerResponse> containerStream = check self.retrieveContainers(requestPath, request);
        return containerStream;
    }

    private function retrieveContainers(string path, http:Request request, string? continuationHeader = (), ContainerResponse[]? 
    containerArray = (), int? maxItemCount = ()) returns @tainted stream<ContainerResponse>|error {
        if(continuationHeader is string){
            request.setHeader(CONTINUATION_HEADER, continuationHeader);
        }
        var response = self.azureCosmosClient->get(path, request);
        stream<ContainerResponse> containerStream  = [].toStream();
        [json, Headers] jsonresponse = check mapResponseToTuple(response);
        json payload;
        Headers headers;
        [payload,headers] = jsonresponse;
        ContainerResponse[] containers = containerArray == ()? []:<ContainerResponse[]>containerArray;
        if(payload.DocumentCollections is json){
            ContainerResponse[] finalArray = convertToContainerArray(containers, <json[]>payload.DocumentCollections);
            containerStream = (<@untainted>finalArray).toStream();
            if(headers?.continuationHeader != () && finalArray.length() == maxItemCount){            
                containerStream = check self.retrieveContainers(path, request, headers.continuationHeader,finalArray);
            }
        }
        return containerStream;
    }

    # Retrive one collection inside a database
    # + containerId - object of type ResourceProperties
    # + return - If successful, returns Container. Else returns error.  
    public remote function getContainer(string containerId) returns @tainted Container|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, containerId]);
        HeaderParameters header = mapParametersToHeaderType(GET, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        var response = self.azureCosmosClient->get(requestPath, request);
        json jsonreponse = check mapResponseToJson(response);
        Container container = new(self.databaseId, jsonreponse.id.toString(), self.azureConfig); 
        return container;
    }

    //check the return type
    # Delete one collection inside a database
    # + containerId - object of type ResourceProperties
    # + return - If successful, returns boolean specifying 'true' if delete is sucessful. Else returns error. 
    public remote function deleteContainer(string containerId) returns @tainted json|error { 
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        containerId]);
        HeaderParameters header = mapParametersToHeaderType(DELETE, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        var response = self.azureCosmosClient->delete(requestPath, request);
        return check getDeleteResponse(response);
    }
}