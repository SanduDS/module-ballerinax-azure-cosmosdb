import ballerina/http;
//import ballerina/io;

# Azure Cosmos DB Client object.
public  client class Client {
    private string baseUrl;
    private string keyOrResourceToken;
    private string host;
    private string keyType;
    private string tokenVersion;
    private AzureCosmosConfiguration azureConfig;
    private http:Client azureCosmosClient;

    function init(AzureCosmosConfiguration azureConfig) {
        self.azureConfig = azureConfig;
        self.baseUrl = azureConfig.baseUrl;
        self.keyOrResourceToken = azureConfig.keyOrResourceToken;
        self.host = azureConfig.host;
        self.keyType = azureConfig.tokenType;
        self.tokenVersion = azureConfig.tokenVersion;
        http:ClientConfiguration httpClientConfig = {secureSocket: azureConfig.secureSocketConfig};
        self.azureCosmosClient = new (self.baseUrl, httpClientConfig);
    }

    # To create a database inside a resource
    # + databaseId -  id/name for the database
    # + throughputProperties - Optional throughput parameter which will set 'x-ms-offer-throughput' or 
    # 'x-ms-cosmos-offer-autopilot-settings' headers 
    # + return - If successful, returns Database. Else returns error.  
    public remote function createDatabase(string databaseId, ThroughputProperties? throughputProperties = ()) returns 
    @tainted DatabaseResponse|error {
        if(self.keyType == TOKEN_TYPE_RESOURCE) {
            return prepareError(MASTER_KEY_ERROR);
        }
        http:Request request = new;
        string requestPath = prepareUrl([RESOURCE_PATH_DATABASES]);
        HeaderParameters header = mapParametersToHeaderType(POST, requestPath);
        json jsonPayload = {
            id:databaseId
        };
        request.setJsonPayload(jsonPayload);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        request = check setThroughputOrAutopilotHeader(request, throughputProperties);
        var response = self.azureCosmosClient->post(requestPath, request);
        [json, Headers] jsonreponse = check mapResponseToTuple(response);
        return mapJsonToDatabaseType(jsonreponse);   
    }

    # To create a database inside a resource
    # + databaseId -  id/name for the database
    # + throughputProperties - Optional throughput parameter which will set 'x-ms-offer-throughput' or 
    # 'x-ms-cosmos-offer-autopilot-settings' headers
    # + return - If successful, returns Database. Else returns error.  
    public remote function createDatabaseIfNotExist(string databaseId, ThroughputProperties? throughputProperties = ()) 
    returns @tainted DatabaseResponse?|error {
        if(self.keyType == TOKEN_TYPE_RESOURCE) {
            return prepareError(MASTER_KEY_ERROR);
        }
        var result = self->getDatabase(databaseId);
        if(result is error) {
            string status = result.detail()["status"].toString();
            if(status == STATUS_NOT_FOUND_STRING){
                return self->createDatabase(databaseId, throughputProperties);
            } else {
                return prepareError(string `External azure error ${status}`);
            }
        }
        return ();  
    }

    # To retrive a given database inside a resource
    # + databaseId -  id/name of the database 
    # + return - If successful, returns Database. Else returns error.  
    public remote function getDatabase(string databaseId) returns @tainted Database|error {
        if(self.keyType == TOKEN_TYPE_RESOURCE) {
            return prepareError(MASTER_KEY_ERROR);
        }
        http:Request request = new;
        string requestPath = prepareUrl([RESOURCE_PATH_DATABASES, databaseId]);
        HeaderParameters header = mapParametersToHeaderType(GET, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        var response = self.azureCosmosClient->get(requestPath, request);
        json jsonreponse = check mapResponseToJson(response);
        Database database = new(jsonreponse.id.toString(), self.azureConfig); 
        return database;  
    }

    # List all databases inside a resource
    # + maxItemCount - Optional integer parameter representing maximum item count.
    # + return - If successful, returns DatabaseList. else returns error. 
    public remote function getDatabases(int? maxItemCount = ()) returns @tainted stream<DatabaseResponse>|error {
        if(self.keyType == TOKEN_TYPE_RESOURCE) {
            return prepareError(MASTER_KEY_ERROR);
        }
        http:Request request = new;
        string requestPath = prepareUrl([RESOURCE_PATH_DATABASES]);
        HeaderParameters header = mapParametersToHeaderType(GET, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        if(maxItemCount is int){
            request.setHeader(MAX_ITEM_COUNT_HEADER, maxItemCount.toString()); 
        }
        stream<DatabaseResponse> databaseStream = check self.retrieveDatabases(requestPath, request, maxItemCount);
        return databaseStream;
    }

    private function retrieveDatabases(string path, http:Request request, int? maxItemCount = (), string? continuationHeader = (), DatabaseResponse[]? 
    databaseArray = ()) returns @tainted stream<DatabaseResponse>|error {
        if(continuationHeader is string){
            request.setHeader(CONTINUATION_HEADER, continuationHeader);
        }
        var response = self.azureCosmosClient->get(path, request);
        stream<DatabaseResponse> databaseStream  = [].toStream();
        var [payload, headers] = check mapResponseToTuple(response);
        DatabaseResponse[] databases = databaseArray == ()? []:<DatabaseResponse[]>databaseArray;
        if(payload.Databases is json){
            DatabaseResponse[] finalArray = convertToDatabaseArray(databases, <json[]>payload.Databases);
            databaseStream = (<@untainted>finalArray).toStream();
            if(headers?.continuationHeader != () && maxItemCount is ()){            
                databaseStream = check self.retrieveDatabases(path, request, (), headers.continuationHeader, finalArray);
            }
        }
        return databaseStream;
    }

    # Delete a given database inside a resource
    # + databaseId -  id/name of the database to retrieve
    # + return - If successful, returns boolean specifying 'true' if delete is sucessful. Else returns error. 
    public remote function deleteDatabase(string databaseId) returns @tainted boolean|error {
        if(self.keyType == TOKEN_TYPE_RESOURCE) {
            return prepareError(MASTER_KEY_ERROR);
        }
        http:Request request = new;
        string requestPath = prepareUrl([RESOURCE_PATH_DATABASES, databaseId]);
        HeaderParameters header = mapParametersToHeaderType(DELETE, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        var response = self.azureCosmosClient->delete(requestPath, request);
        return check getDeleteResponse(response);
    }

    # Gets information of offers inside database account
    # Each Azure Cosmos DB collection is provisioned with an associated performance level represented as an 
    # Offer resource in the REST model. Azure Cosmos DB supports offers representing both user-defined performance 
    # levels and pre-defined performance levels. 
    # + maxItemCount - Optional integer parameter representing maximum item count.
    # + return - If successful, returns a OfferList. Else returns error.
    public remote function listOffers(int? maxItemCount = ()) returns @tainted stream<Offer>|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_OFFER]);       
        HeaderParameters header = mapParametersToHeaderType(GET, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        if(maxItemCount is int){
            request.setHeader(MAX_ITEM_COUNT_HEADER, maxItemCount.toString()); 
        }
        stream<Offer> storedProcedureStream = check self.retriveOffers(requestPath, request, maxItemCount);
        return storedProcedureStream;
    }

    private function retriveOffers(string path, http:Request request, int? maxItemCount = (),string? continuationHeader = (), Offer[]? 
    storedProcedureArray = ()) returns @tainted stream<Offer>|error {
        if(continuationHeader is string){
            request.setHeader(CONTINUATION_HEADER, continuationHeader);
        }
        var response = self.azureCosmosClient->get(path, request);
        stream<Offer> storedProcedureStream  = [].toStream();
        var [payload, headers] = check mapResponseToTuple(response);
        Offer[] storedProcedures = storedProcedureArray == ()? []:<Offer[]>storedProcedureArray;
        if(payload.Offers is json){
            Offer[] finalArray = ConvertToOfferArray(storedProcedures, <json[]>payload.Offers);
            storedProcedureStream = (<@untainted>finalArray).toStream();
            if(headers?.continuationHeader != () && maxItemCount is ()){            
                storedProcedureStream = check self.retriveOffers(path, request, (), headers.continuationHeader,finalArray);
            }
        }// handle else
        return storedProcedureStream;
    }

    # Get information of an offer
    # + offerId - the id of offer
    # + return - If successful, returns a Offer. Else returns error.
    public remote function getOffer(string offerId) returns @tainted Offer|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_OFFER, offerId]);       
        HeaderParameters header = mapOfferHeaderType(GET, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        var response = self.azureCosmosClient->get(requestPath, request);
        [json, Headers] jsonResponse = check mapResponseToTuple(response);
        return mapJsonToOfferType(jsonResponse);
    }

    # Replace an existing offer
    # + offer - an object of type Offer
    # + offerType - Optional parameter offer type 
    # + return - If successful, returns a Offer. Else returns error.
    public remote function replaceOffer(Offer offer, string? offerType = ()) returns @tainted Offer|error {
        http:Request request = new;
        string requestPath = prepareUrl([RESOURCE_PATH_OFFER, offer.id]);       
        HeaderParameters header = mapOfferHeaderType(PUT, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        json jsonPaylod = {
            "offerVersion": offer.offerVersion, 
            "content": offer.content, 
            "resource": offer.resourceSelfLink, 
            "offerResourceId": offer.offerResourceId, 
            "id": offer.id, 
            "_rid": offer?.resourceId
        };
        if(offerType is string && offer.offerVersion == "V1") {
            json selectedType = {
                "offerType": offerType
            };
            jsonPaylod = check jsonPaylod.mergeJson(selectedType);
        }
        request.setJsonPayload(jsonPaylod);
        var response = self.azureCosmosClient->put(requestPath, request);
        [json, Headers] jsonResponse = check mapResponseToTuple(response);
        return mapJsonToOfferType(jsonResponse);
    }

    # Perform queries on Offer resources
    # + sqlQuery - the CQL query to execute
    # + return - If successful, returns a json. Else returns error.
    public remote function queryOffer(Query sqlQuery) returns @tainted stream<json>|error {
        http:Request request = new;
        string requestPath = prepareUrl([RESOURCE_PATH_OFFER]);
        HeaderParameters header = mapParametersToHeaderType(POST, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        request.setJsonPayload(<json>sqlQuery.cloneWithType(json));
        request = check setHeadersForQuery(request);
        var response = self.azureCosmosClient->post(requestPath, request);
        stream<json> jsonresponse = check mapResponseToJsonStream(response);
        return (jsonresponse);
    }
}
