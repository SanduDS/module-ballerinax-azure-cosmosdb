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
        var [payload, headers] = check mapResponseToTuple(response);
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

    # Create a user in a database
    # + userId - the id which should be given to the new user
    # + return - If successful, returns a User. Else returns error.
    public remote function createUser(string userId) returns @tainted 
    User|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_USER]);       
        HeaderParameters header = mapParametersToHeaderType(POST, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        json reqBody = {
            id:userId
        };
        request.setJsonPayload(reqBody);
        var response = self.azureCosmosClient->post(requestPath, request);
        [json, Headers] jsonResponse = check mapResponseToTuple(response);
        return mapJsonToUserType(jsonResponse);     
    }
    
    # Replace the id of an existing user for a database
    # + userId - the id which should be given to the new user
    # + newUserId - the new id for the user
    # + return - If successful, returns a User. Else returns error.
    public remote function replaceUserId(string userId, string newUserId) returns 
    @tainted User|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_USER, userId]);       
        HeaderParameters header = mapParametersToHeaderType(PUT, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        json reqBody = {
            id:newUserId
        };
        request.setJsonPayload(reqBody);
        var response = self.azureCosmosClient->put(requestPath, request);
        [json, Headers] jsonResponse = check mapResponseToTuple(response);
        return mapJsonToUserType(jsonResponse); 
    }

    # To get information of a user from a database
    # + userId - the id of user to get information
    # + return - If successful, returns a User. Else returns error.
    public remote function getUser(string userId) returns @tainted User|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_USER, userId]);
        HeaderParameters header = mapParametersToHeaderType(GET, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        var response = self.azureCosmosClient->get(requestPath, request);
        [json, Headers] jsonResponse = check mapResponseToTuple(response);
        return mapJsonToUserType(jsonResponse);      
    }

    # Lists users in a database account
    # + maxItemCount -
    # + return - If successful, returns a UserList. Else returns error.
    public remote function listUsers(int? maxItemCount = ()) returns @tainted stream<User>|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_USER]);
        HeaderParameters header = mapParametersToHeaderType(GET, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        if(maxItemCount is int){
            request.setHeader(MAX_ITEM_COUNT_HEADER, maxItemCount.toString()); 
        }
        stream<User> storedProcedureStream = check self.retrieveUsers(requestPath, request);
        return storedProcedureStream;       
    }

    private function retrieveUsers(string path, http:Request request, string? continuationHeader = (), User[]? 
    storedProcedureArray = (), int? maxItemCount = ()) returns @tainted stream<User>|error {
        if(continuationHeader is string){
            request.setHeader(CONTINUATION_HEADER, continuationHeader);
        }
        var response = self.azureCosmosClient->get(path, request);
        stream<User> storedProcedureStream  = [].toStream();
        var [payload, headers] = check mapResponseToTuple(response);
        User[] storedProcedures = storedProcedureArray == ()? []:<User[]>storedProcedureArray;
        if(payload.Users is json){
            User[] finalArray = convertToUserArray(storedProcedures, <json[]>payload.Users);
            storedProcedureStream = (<@untainted>finalArray).toStream();
            if(headers?.continuationHeader != () && finalArray.length() == maxItemCount){            
                storedProcedureStream = check self.retrieveUsers(path, request, headers.continuationHeader,finalArray);
            }
        }// handle else
        return storedProcedureStream;
    }

    # Delete a user from a database account
    # + userId - the id of user to delete
    # + return - If successful, returns boolean specifying 'true' if delete is sucessful. Else returns error. 
    public remote function deleteUser(string userId) returns @tainted 
    boolean|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_USER, userId]);       
        HeaderParameters header = mapParametersToHeaderType(DELETE, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        var response = self.azureCosmosClient->delete(requestPath, request);
        return check getDeleteResponse(response);
    }

    # Create a permission for a user 
    # + userId - the id of user to which the permission belongs
    # + permission - object of type Permission
    # + validityPeriod - optional validity period parameter which specify  ttl
    # + return - If successful, returns a Permission. Else returns error.
    public remote function createPermission(string userId, Permission permission, int? validityPeriod = ()) returns @tainted Permission|error {
        if(self.keyType == TOKEN_TYPE_RESOURCE) {
            return prepareError(MASTER_KEY_ERROR);
        }
        http:Request request = new;
        string requestPath = prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_USER, userId, 
        RESOURCE_PATH_PERMISSION]);       
        HeaderParameters header = mapParametersToHeaderType(POST, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        if(validityPeriod is int) {
            request = check setExpiryHeader(request, validityPeriod);
        }
        json jsonPayload = {
            "id" : permission.id, 
            "permissionMode" : permission.permissionMode, 
            "resource": permission.resourcePath
        };
        request.setJsonPayload(jsonPayload);
        var response = self.azureCosmosClient->post(requestPath, request);
        [json, Headers] jsonResponse = check mapResponseToTuple(response);
        return mapJsonToPermissionType(jsonResponse);
    }

    # Replace an existing permission
    # + userId - the id of user to which the permission belongs
    # + permission - object of type Permission
    # + validityPeriod - optional validity period parameter which specify  ttl
    # + return - If successful, returns a Permission. Else returns error.
    public remote function replacePermission(string userId, @tainted Permission permission, int? validityPeriod = ()) returns @tainted Permission|error {
        if(self.keyType == TOKEN_TYPE_RESOURCE) {
            return prepareError(MASTER_KEY_ERROR);
        }
        http:Request request = new;
        string requestPath = prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_USER, userId, 
        RESOURCE_PATH_PERMISSION, permission.id]);       
        HeaderParameters header = mapParametersToHeaderType(PUT, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        if(validityPeriod is int) {
            request = check setExpiryHeader(request, validityPeriod);
        }
        json jsonPayload = {
            "id" : permission.id, 
            "permissionMode" : permission.permissionMode, 
            "resource": permission.resourcePath
        };
        request.setJsonPayload(<@untainted>jsonPayload);
        var response = self.azureCosmosClient->put(requestPath, request);
        [json, Headers] jsonResponse = check mapResponseToTuple(response);
        return mapJsonToPermissionType(jsonResponse);
    }

    # Lists permissions belong to a user
    # + userId - the id of user to the permissions belong
    # + maxItemCount -
    # + return - If successful, returns a PermissionList. Else returns error.
    public remote function listPermissions(string userId, int? maxItemCount = ()) returns @tainted 
    stream<Permission>|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_USER, userId, 
        RESOURCE_PATH_PERMISSION]);       
        HeaderParameters header = mapParametersToHeaderType(GET, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        if(maxItemCount is int){
            request.setHeader(MAX_ITEM_COUNT_HEADER, maxItemCount.toString()); 
        }
        stream<Permission> storedProcedureStream = check self.retrievePermissions(requestPath, request);
        return storedProcedureStream;
    }

    private function retrievePermissions(string path, http:Request request, string? continuationHeader = (), Permission[]? 
    storedProcedureArray = (), int? maxItemCount = ()) returns @tainted stream<Permission>|error {
        if(continuationHeader is string){
            request.setHeader(CONTINUATION_HEADER, continuationHeader);
        }
        var response = self.azureCosmosClient->get(path, request);
        stream<Permission> storedProcedureStream  = [].toStream();
        var [payload, headers] = check mapResponseToTuple(response);
        Permission[] storedProcedures = storedProcedureArray == ()? []:<Permission[]>storedProcedureArray;
        if(payload.Permissions is json){
            Permission[] finalArray = convertToPermissionArray(storedProcedures, <json[]>payload.Permissions);
            storedProcedureStream = (<@untainted>finalArray).toStream();
            if(headers?.continuationHeader != () && finalArray.length() == maxItemCount){            
                storedProcedureStream = check self.retrievePermissions(path, request, headers.continuationHeader,finalArray);
            }
        }// handle else
        return storedProcedureStream;
    }

    # To get information of a permission belongs to a user
    # + userId - the id of user to the permission belongs
    # + permissionId - object of type Permission
    # + return - If successful, returns a Permission. Else returns error.
    public remote function getPermission(string userId, string permissionId)
    returns @tainted Permission|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_USER, userId, 
        RESOURCE_PATH_PERMISSION, permissionId]);       
        HeaderParameters header = mapParametersToHeaderType(GET, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        var response = self.azureCosmosClient->get(requestPath, request);
        [json, Headers] jsonResponse = check mapResponseToTuple(response);
        return mapJsonToPermissionType(jsonResponse);
    }

    # Deletes a permission belongs to a user
    # + userId - the id of user to the permission belongs
    # + permissionId - id of the permission to delete
    # + return - If successful, returns boolean specifying 'true' if delete is sucessful. Else returns error. 
    public remote function deletePermission(string userId, string permissionId) 
    returns @tainted boolean|error {
        http:Request request = new;
        string requestPath =  prepareUrl([RESOURCE_PATH_DATABASES, self.databaseId, RESOURCE_PATH_USER, userId, 
        RESOURCE_PATH_PERMISSION, permissionId]);       
        HeaderParameters header = mapParametersToHeaderType(DELETE, requestPath);
        request = check setHeaders(request, self.host, self.keyOrResourceToken, self.keyType, self.tokenVersion, header);
        var response = self.azureCosmosClient->delete(requestPath, request);
        return check getDeleteResponse(response);
    }
}