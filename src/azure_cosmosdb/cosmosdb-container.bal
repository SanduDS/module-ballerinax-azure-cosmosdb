import ballerina/http;

public client class Container  {
    
    private string baseUrl;
    private string keyOrResourceToken;
    private string host;
    private string keyType;
    private string tokenVersion;
    private AzureCosmosConfiguration azureConfig;
    private string containerId;
    private string databaseId;

    public http:Client azureCosmosClient;

    function init(string databaseId, string containerId, AzureCosmosConfiguration azureConfig) {
        self.azureConfig = azureConfig;
        self.containerId = containerId;
        self.databaseId =  databaseId;
        self.baseUrl = azureConfig.baseUrl;
        self.keyOrResourceToken = azureConfig.keyOrResourceToken;
        self.host = azureConfig.host;
        self.keyType = azureConfig.tokenType;
        self.tokenVersion = azureConfig.tokenVersion;
        http:ClientConfiguration httpClientConfig = {secureSocket: azureConfig.secureSocketConfig};
        self.azureCosmosClient = new (self.baseUrl, httpClientConfig);
    }

    # Retrieve a list of partition key ranges for the collection
    # + return - If successful, returns PartitionKeyList. Else returns error.  
    public remote function getPartitionKeyRanges() returns @tainted PartitionKeyList|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_PK_RANGES]);
        HeaderParameters header = mapParametersToHeaderType(GET, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        var response = self.azureCosmosClient->get(requestPath, request);
        [json, Headers] jsonreponse = check mapResponseToTuple(response);
        return mapJsonToPartitionKeyListType(jsonreponse);
    }

    # Create a Document inside a collection
    # + document - object of type Document 
    # + requestOptions - object of type RequestHeaderOptions
    # + return - If successful, returns Document. Else returns error.  
    public remote function createDocument(Document document, RequestHeaderOptions? requestOptions = ()) returns @tainted Document|error {
        http:Request request = new;
        string requestPath = prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_DOCUMENTS]);
        HeaderParameters header = mapParametersToHeaderType(POST, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        request = check setPartitionKeyHeader(request, document.partitionKey);
        if(requestOptions is RequestHeaderOptions) {
            request = check setRequestOptions(request, requestOptions);
        }
        json jsonPayload = {
            id: document.id
        };  
        jsonPayload = check jsonPayload.mergeJson(document.documentBody);     
        request.setJsonPayload(jsonPayload);
        var response = self.azureCosmosClient->post(requestPath, request);
        [json, Headers] jsonreponse = check mapResponseToTuple(response);
        return mapJsonToDocumentType(jsonreponse);
    }

    # Replace a document inside a collection
    # + document - object of type Document 
    # + requestOptions - object of type RequestHeaderOptions
    # set x-ms-documentdb-partitionkey header
    # + return - If successful, returns a Document. Else returns error. 
    public remote function replaceDocument(@tainted Document document, RequestHeaderOptions? requestOptions = ()) returns 
    @tainted Document|error {         
        http:Request request = new;
        string requestPath = prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_DOCUMENTS, document.id]);
        HeaderParameters header = mapParametersToHeaderType(PUT, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        request = check setPartitionKeyHeader(request, document.partitionKey);
        if(requestOptions is RequestHeaderOptions) {
            request = check setRequestOptions(request, requestOptions);
        }
        json jsonPayload = {
            id: document.id
        };  
        jsonPayload = check jsonPayload.mergeJson(document.documentBody); 
        request.setJsonPayload(<@untainted>jsonPayload);
        var response = self.azureCosmosClient->put(requestPath, request);
        [json, Headers] jsonreponse = check mapResponseToTuple(response);
        return mapJsonToDocumentType(jsonreponse);
    }

    # List one document inside a collection
    # + documentId - id of  Document, 
    # + partitionKey - array containing value of parition key field.
    # + requestOptions - object of type RequestHeaderOptions
    # + return - If successful, returns Document. Else returns error.  
    public remote function getDocument(string documentId, any[] partitionKey, RequestHeaderOptions? requestOptions = ()) 
    returns @tainted Document|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_DOCUMENTS, documentId]);
        HeaderParameters header = mapParametersToHeaderType(GET, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        request = check setPartitionKeyHeader(request, partitionKey);
        if requestOptions is RequestHeaderOptions {
            request = check setRequestOptions(request, requestOptions);
        }
        var response = self.azureCosmosClient->get(requestPath, request);
        [json, Headers] jsonreponse = check mapResponseToTuple(response);
        return mapJsonToDocumentType(jsonreponse);
    }

    # List all the documents inside a collection
    # + requestOptions - object of type RequestHeaderOptions
    # + maxItemCount -
    # + return - If successful, returns DocumentList. Else returns error. 
    public remote function getDocumentList(RequestHeaderOptions? requestOptions = (), int? maxItemCount = ()) 
    returns @tainted stream<Document>|error { 
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_DOCUMENTS]);
        HeaderParameters header = mapParametersToHeaderType(GET, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        if requestOptions is RequestHeaderOptions {
            request = check setRequestOptions(request, requestOptions);
        }
        if(maxItemCount is int){
            request.setHeader(MAX_ITEM_COUNT_HEADER, maxItemCount.toString()); 
        }
        stream<Document> documentStream = check self.retrieveDocuments(requestPath, request);
        return documentStream; 
    }

    private function retrieveDocuments(string path, http:Request request, string? continuationHeader = (), Document[]? 
    documentArray = (), int? maxItemCount = ()) returns @tainted stream<Document>|error {
        if(continuationHeader is string){
            request.setHeader(CONTINUATION_HEADER, continuationHeader);
        }
        var response = self.azureCosmosClient->get(path, request);
        stream<Document> documentStream  = [].toStream();
        var [payload, headers] = check mapResponseToTuple(response);
        Document[] documents = documentArray is Document[]?<Document[]>documentArray:[];
        if(payload.Documents is json){
            Document[] finalArray = convertToDocumentArray(documents, <json[]>payload.Documents);
            documentStream = (<@untainted>finalArray).toStream();
            if(headers?.continuationHeader != () && finalArray.length() == maxItemCount){            
                documentStream = check self.retrieveDocuments(path, request, headers.continuationHeader,finalArray);
            }
        }
        return documentStream;
    }

    # Delete a document inside a collection
    # + documentId - id of the Document 
    # + partitionKey - array containing value of parition key field.
    # + return - If successful, returns boolean specifying 'true' if delete is sucessful. Else returns error. 
    public remote function deleteDocument(string documentId, any[] partitionKey) 
    returns @tainted boolean|error {  
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_DOCUMENTS, documentId]);
        HeaderParameters header = mapParametersToHeaderType(DELETE, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        request = check setPartitionKeyHeader(request, partitionKey);
        var response = self.azureCosmosClient->delete(requestPath, request);
        return check getDeleteResponse(response);
    }

    # Query documents inside a collection
    # + sqlQuery - json object of type Query containing the CQL query
    # + requestOptions - object of type RequestOptions
    # + partitionKey - the value provided for the partition key specified in the document
    # + return - If successful, returns a json. Else returns error. 
    public remote function queryDocuments(any[] partitionKey, Query sqlQuery, RequestHeaderOptions? requestOptions = ()) 
    returns @tainted stream<json>|error {
        http:Request request = new;
        string requestPath = prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_DOCUMENTS]);
        HeaderParameters header = mapParametersToHeaderType(POST, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        request = check setPartitionKeyHeader(request, partitionKey);
        request.setPayload(<json>sqlQuery.cloneWithType(json));
        request = check setHeadersForQuery(request);
        var response = self.azureCosmosClient->post(requestPath, request);
        stream<json> jsonresponse = check mapResponseToJsonStream(response);
        return jsonresponse;
    }

    # Create a new stored procedure inside a collection
    # A stored procedure is a piece of application logic written in JavaScript that 
    # is registered and executed against a collection as a single transaction.
    # + storedProcedure - object of type StoredProcedure
    # + return - If successful, returns a StoredProcedure. Else returns error. 
    public remote function createStoredProcedure(StoredProcedure storedProcedure) returns @tainted StoredProcedure|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_STORED_POCEDURES]);
        HeaderParameters header = mapParametersToHeaderType(POST, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        request.setJsonPayload(<json>storedProcedure.cloneWithType(json));
        var response = self.azureCosmosClient->post(requestPath, request);
        [json, Headers] jsonResponse = check mapResponseToTuple(response);
        return mapJsonToStoredProcedureType(jsonResponse);    
    }

    # Replace a stored procedure with new one inside a collection
    # + storedProcedure - object of type StoredProcedure
    # + return - If successful, returns a StoredProcedure. Else returns error. 
    public remote function replaceStoredProcedure(@tainted StoredProcedure storedProcedure) returns @tainted StoredProcedure|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_STORED_POCEDURES, storedProcedure.id]);
        HeaderParameters header = mapParametersToHeaderType(PUT, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        request.setJsonPayload(<@untainted><json>storedProcedure.cloneWithType(json));
        var response = self.azureCosmosClient->put(requestPath, request);
        [json, Headers] jsonResponse = check mapResponseToTuple(response);
        return mapJsonToStoredProcedureType(jsonResponse);  
    }

    # List all stored procedures inside a collection
    # + maxItemCount -
    # + return - If successful, returns a StoredProcedureList. Else returns error. 
    public remote function listStoredProcedures(int? maxItemCount = ()) returns @tainted stream<StoredProcedure>|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_STORED_POCEDURES]);
        HeaderParameters header = mapParametersToHeaderType(GET, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        if(maxItemCount is int){
            request.setHeader(MAX_ITEM_COUNT_HEADER, maxItemCount.toString()); 
        }
        stream<StoredProcedure> storedProcedureStream = check self.retrieveStoredProcedures(requestPath, request);
        return storedProcedureStream;         
    }

    private function retrieveStoredProcedures(string path, http:Request request, string? continuationHeader = (), StoredProcedure[]? 
    storedProcedureArray = (), int? maxItemCount = ()) returns @tainted stream<StoredProcedure>|error {
        if(continuationHeader is string){
            request.setHeader(CONTINUATION_HEADER, continuationHeader);
        }
        var response = self.azureCosmosClient->get(path, request);
        stream<StoredProcedure> storedProcedureStream  = [].toStream();
        var [payload, headers] = check mapResponseToTuple(response);
        StoredProcedure[] storedProcedures = storedProcedureArray == ()? []:<StoredProcedure[]>storedProcedureArray;
        if(payload.StoredProcedures is json){
            StoredProcedure[] finalArray = convertToStoredProcedureArray(storedProcedures, <json[]>payload.StoredProcedures);
            storedProcedureStream = (<@untainted>finalArray).toStream();
            if(headers?.continuationHeader != () && finalArray.length() == maxItemCount){            
                storedProcedureStream = check self.retrieveStoredProcedures(path, request, headers.continuationHeader,finalArray);
            }
        }
        return storedProcedureStream;
    }

    # Delete a stored procedure inside a collection
    # + storedProcedureId - id of the stored procedure to delete
    # + return - If successful, returns boolean specifying 'true' if delete is sucessful. Else returns error. 
    public remote function deleteStoredProcedure(string storedProcedureId) returns 
    @tainted boolean|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_STORED_POCEDURES, storedProcedureId]);        
        HeaderParameters header = mapParametersToHeaderType(DELETE, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        var response = self.azureCosmosClient->delete(requestPath, request);
        return check getDeleteResponse(response);
    }

    # Execute a stored procedure inside a collection
    # ***********function only works correctly for string parameters************
    # + storedProcedureId - id of the stored procedure to execute
    # + parameters - The list of function paramaters to pass to javascript function as an array.
    # + return - If successful, returns json with the output from the executed funxtion. Else returns error. 
    public remote function executeStoredProcedure(string storedProcedureId, any[]? parameters) returns @tainted json|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_STORED_POCEDURES, storedProcedureId]);       
        HeaderParameters header = mapParametersToHeaderType(POST, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        request.setTextPayload(parameters.toString());
        var response = self.azureCosmosClient->post(requestPath, request);
        json jsonreponse = check mapResponseToJson(response);
        return jsonreponse;   
    }

    # Create a new user defined function inside a collection
    # A user-defined function (UDF) is a side effect free piece of application logic written in JavaScript. 
    # + userDefinedFunction - object of type UserDefinedFunction
    # + return - If successful, returns a UserDefinedFunction. Else returns error. 
    public remote function createUserDefinedFunction(UserDefinedFunction userDefinedFunction) returns @tainted UserDefinedFunction|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_UDF]);       
        HeaderParameters header = mapParametersToHeaderType(POST, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        request.setJsonPayload(<json>userDefinedFunction.cloneWithType(json));
        var response = self.azureCosmosClient->post(requestPath, request);
        [json, Headers] jsonResponse = check mapResponseToTuple(response);
        return mapJsonToUserDefinedFunctionType(jsonResponse);      
    }

    # Replace an existing user defined function inside a collection
    # + userDefinedFunction - object of type UserDefinedFunction
    # + return - If successful, returns a UserDefinedFunction. Else returns error. 
    public remote function replaceUserDefinedFunction(@tainted UserDefinedFunction userDefinedFunction) returns @tainted UserDefinedFunction|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_UDF, userDefinedFunction.id]);      
        HeaderParameters header = mapParametersToHeaderType(PUT, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        request.setJsonPayload(<@untainted><json>userDefinedFunction.cloneWithType(json));
        var response = self.azureCosmosClient->put(requestPath, request);
        [json, Headers] jsonResponse = check mapResponseToTuple(response);
        return mapJsonToUserDefinedFunctionType(jsonResponse);      
    }

    # Get a list of existing user defined functions inside a collection
    # + maxItemCount - 
    # + return - If successful, returns a UserDefinedFunctionList. Else returns error. 
    public remote function listUserDefinedFunctions(int? maxItemCount = ()) returns @tainted stream<UserDefinedFunction>|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_UDF]);
        HeaderParameters header = mapParametersToHeaderType(GET, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        if(maxItemCount is int){
            request.setHeader(MAX_ITEM_COUNT_HEADER, maxItemCount.toString()); 
        }
        stream<UserDefinedFunction> storedProcedureStream = check self.retrieveUserDefinedFunctions(requestPath, request);
        return storedProcedureStream;   
    }

    private function retrieveUserDefinedFunctions(string path, http:Request request, string? continuationHeader = (), UserDefinedFunction[]? 
    storedProcedureArray = (), int? maxItemCount = ()) returns @tainted stream<UserDefinedFunction>|error {
        if(continuationHeader is string){
            request.setHeader(CONTINUATION_HEADER, continuationHeader);
        }
        var response = self.azureCosmosClient->get(path, request);
        stream<StoredProcedure> storedProcedureStream  = [].toStream();
        var [payload, headers] = check mapResponseToTuple(response);
        UserDefinedFunction[] storedProcedures = storedProcedureArray == ()? []:<UserDefinedFunction[]>storedProcedureArray;
        if(payload.UserDefinedFunctions is json){
            UserDefinedFunction[] finalArray = convertsToUserDefinedFunctionArray(storedProcedures, <json[]>payload.UserDefinedFunctions);
            storedProcedureStream = (<@untainted>finalArray).toStream();
            if(headers?.continuationHeader != () && finalArray.length() == maxItemCount){            
                storedProcedureStream = check self.retrieveUserDefinedFunctions(path, request, headers.continuationHeader,finalArray);
            }
        }
        return storedProcedureStream;
    }

    # Delete an existing user defined function inside a collection
    # + userDefinedFunctionid - id of UDF to delete
    # + return - If successful, returns boolean specifying 'true' if delete is sucessful. Else returns error. 
    public remote function deleteUserDefinedFunction(string userDefinedFunctionid) returns @tainted boolean|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_UDF, userDefinedFunctionid]);        
        HeaderParameters header = mapParametersToHeaderType(DELETE, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        var response = self.azureCosmosClient->delete(requestPath, request);
        return check getDeleteResponse(response);
    }

    # Create a trigger inside a collection
    # Triggers are pieces of application logic that can be executed before (pre-triggers) and after (post-triggers) 
    # creation, deletion, and replacement of a document. Triggers are written in JavaScript. 
    # + trigger - object of type Trigger
    # + return - If successful, returns a Trigger. Else returns error. 
    public remote function createTrigger(Trigger trigger) returns @tainted Trigger|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_TRIGGER]);       
        HeaderParameters header = mapParametersToHeaderType(POST, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        request.setJsonPayload(<json>trigger.cloneWithType(json));
        var response = self.azureCosmosClient->post(requestPath, request);
        [json, Headers] jsonResponse = check mapResponseToTuple(response);
        return mapJsonToTriggerType(jsonResponse);      
    }
    
    # Replace an existing trigger inside a collection
    # + trigger - object of type Trigger
    # + return - If successful, returns a Trigger. Else returns error. 
    public remote function replaceTrigger(@tainted Trigger trigger) returns @tainted Trigger|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_TRIGGER, trigger.id]);       
        HeaderParameters header = mapParametersToHeaderType(PUT, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        request.setJsonPayload(<@untainted><json>trigger.cloneWithType(json));
        var response = self.azureCosmosClient->put(requestPath, request);
        [json, Headers] jsonResponse = check mapResponseToTuple(response);
        return mapJsonToTriggerType(jsonResponse); 
    }

    # List existing triggers inside a collection
    # + maxItemCount -
    # + return - If successful, returns a TriggerList. Else returns error. 
    public remote function listTriggers(int? maxItemCount = ()) returns @tainted stream<Trigger>|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_TRIGGER]);
        HeaderParameters header = mapParametersToHeaderType(GET, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        if(maxItemCount is int){
            request.setHeader(MAX_ITEM_COUNT_HEADER, maxItemCount.toString()); 
        }
        stream<Trigger> storedProcedureStream = check self.retrieveTriggers(requestPath, request);
        return storedProcedureStream;       
    }
    
    private function retrieveTriggers(string path, http:Request request, string? continuationHeader = (), Trigger[]? 
    storedProcedureArray = (), int? maxItemCount = ()) returns @tainted stream<Trigger>|error {
        if(continuationHeader is string){
            request.setHeader(CONTINUATION_HEADER, continuationHeader);
        }
        var response = self.azureCosmosClient->get(path, request);
        stream<Trigger> storedProcedureStream  = [].toStream();
        var [payload, headers] = check mapResponseToTuple(response);
        Trigger[] storedProcedures = storedProcedureArray == ()? []:<Trigger[]>storedProcedureArray;
        if(payload.Triggers is json){
            Trigger[] finalArray = convertToTriggerArray(storedProcedures, <json[]>payload.Triggers);
            storedProcedureStream = (<@untainted>finalArray).toStream();
            if(headers?.continuationHeader != () && finalArray.length() == maxItemCount){            
                storedProcedureStream = check self.retrieveTriggers(path, request, headers.continuationHeader,finalArray);
            }
        }
        return storedProcedureStream;
    }

    # Delete an existing trigger inside a collection
    # + triggerId - id of the trigger to be deleted
    # + return - If successful, returns boolean specifying 'true' if delete is sucessful. Else returns error. 
    public remote function deleteTrigger(string triggerId) returns @tainted 
    boolean|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_COLLECTIONS, 
        self.containerId, RESOURCE_PATH_TRIGGER, triggerId]);       
        HeaderParameters header = mapParametersToHeaderType(DELETE, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        var response = self.azureCosmosClient->delete(requestPath, request);
        return check getDeleteResponse(response);
    }
}