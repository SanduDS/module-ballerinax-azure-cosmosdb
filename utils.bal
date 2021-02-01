// Copyright (c) 2020 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
//
// WSO2 Inc. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
import ballerina/time;
import ballerina/http;
import ballerina/crypto;
import ballerina/encoding;
import ballerina/stringutils;
import ballerina/lang.'string as str;
import ballerina/lang.array as array;
import ballerina/java;
import ballerina/log;

// Validate if the base URL is an empty string
// 
//  + url - the URL from which we want to extract resource type
//
isolated function validateBaseUrl(string url) returns string|error {
    if (url != "") {
        return url;
    }
    return prepareUserError(EMPTY_BASE_URL_ERROR);
}

// Validate if the  is an empty string
// 
//  + token - the token provided by the user to access Cosmos DB.
//
isolated function validateMasterToken(string token) returns string|error {
    if (token != "") {
        byte[]|error encodedValue = encoding:decodeBase64Url(token);
        if (encodedValue is byte[]) {
            return token;
        } else {
            return prepareUserError(INVALID_MASTER_TOKEN_ERROR);
        }
    }
    return prepareUserError(EMPTY_MASTER_TOKEN_ERROR);
}

//  Extract the type of token used for accessing the Cosmos DB.
// 
//  + token - the token provided by the user to access Cosmos DB.
//
function getTokenType(string token) returns string {
    boolean contain = stringutils:contains(token, TOKEN_TYPE_RESOURCE);
    if (contain) {
        return TOKEN_TYPE_RESOURCE;
    } else {
        return TOKEN_TYPE_MASTER;
    }
}

//  Extract the host of the cosmos db from the base url.
// 
//  + url - the Base URL given by the user from which we want to extract host.
//  + return - string representing the resource id.
//
isolated function getHost(string url) returns string {
    string replaced = stringutils:replaceFirst(url, HTTPS_REGEX, EMPTY_STRING);
    int? lastIndex = str:lastIndexOf(replaced, FORWARD_SLASH);
    if (lastIndex is int) {
        replaced = replaced.substring(0, lastIndex);
    }
    return replaced;
}

//  Extract the resource type related to cosmos db from a given url
// 
//  + url - the URL from which we want to extract resource type
//  + return - string representing the resource type
//
isolated function getResourceType(string url) returns string {
    string resourceType = EMPTY_STRING;
    string[] urlParts = stringutils:split(url, FORWARD_SLASH);
    int count = urlParts.length() - 1;
    if (count % 2 != 0) {
        resourceType = urlParts[count];
        if (count > 1) {
            int? lastIndex = str:lastIndexOf(url, FORWARD_SLASH);
        }
    } else {
        resourceType = urlParts[count - 1];
    }
    return resourceType;
}

//  Extract the resource type related to cosmos db from a given url
// 
//  + url - the URL from which we want to extract resource type
//  + return - string representing the resource id
//
isolated function getResourceId(string url) returns string {
    string resourceId = EMPTY_STRING;
    string[] urlParts = stringutils:split(url, FORWARD_SLASH);
    int count = urlParts.length() - 1;
    string resourceType = getResourceType(url);
    if (resourceType == RESOURCE_TYPE_OFFERS) {
        if (count % 2 != 0) {
            resourceId = EMPTY_STRING;
        } else {
            int? lastIndex = str:lastIndexOf(url, FORWARD_SLASH);
            if (lastIndex is int) {
                resourceId = str:substring(url, lastIndex + 1);
            }
        }
        return resourceId.toLowerAscii();
    } else {
        if (count % 2 != 0) {
            if (count > 1) {
                int? lastIndex = str:lastIndexOf(url, FORWARD_SLASH);
                if (lastIndex is int) {
                    resourceId = str:substring(url, 1, lastIndex);
                }
            }
        } else {
            resourceId = str:substring(url, 1);
        }
        return resourceId;
    }
}

//  Prepare the url out of a given string array 
// 
//  + paths - array of strings with path of the url
//  + return - string representing the complete url
//
isolated function prepareUrl(string[] paths) returns string {
    string url = EMPTY_STRING;
    if (paths.length() > 0) {
        foreach var path in paths {
            if (!path.startsWith(FORWARD_SLASH)) {
                url = url + FORWARD_SLASH;
            }
            url = url + path;
        }
    }
    return <@untainted>url;
}

isolated function createRequest(http:Request request, (DocumentCreateOptions|DocumentReplaceOptions|DocumentGetOptions|
        DocumentListOptions|ResourceReadOptions|ResourceQueryOptions|ResourceDeleteOptions)? requestOptions) returns error? {
    if (requestOptions != ()) {
        check setRequestOptions(request, requestOptions);
    }
}

//  Attach mandatory basic headers to call a REST endpoint.
//  
//  + request - http:Request to add headers to
//  + host - the host to which the request is sent
//  + keyToken - master or resource token
//  + tokenType - denotes the type of token: master or resource.
//  + tokenVersion - denotes the version of the token, currently 1.0.
//  + httpVerb - The HTTP verb of the request the headers are set to.
//  + requestPath - Request path of the request.
//  + return - If successful, returns same http:Request with newly appended headers. Else returns error.
//
isolated function setMandatoryHeaders(http:Request request, string host, string keyToken, string tokenType, 
        string tokenVersion, string httpVerb, string requestPath) returns error? {
    HeaderParameters params = mapParametersToHeaderType(httpVerb, requestPath);
    request.setHeader(API_VERSION_HEADER, params.apiVersion);
    request.setHeader(HOST_HEADER, host);
    request.setHeader(ACCEPT_HEADER, ACCEPT_ALL);
    request.setHeader(CONNECTION_HEADER, CONNECTION_KEEP_ALIVE);
    string? date = getTime();
    if (date is string) {
        request.setHeader(DATE_HEADER, date);
        string? signature = ();
        if (tokenType.toLowerAscii() == TOKEN_TYPE_MASTER) {
            signature = check generateMasterTokenSignature(params.verb, params.resourceType, params.resourceId, keyToken, 
                    tokenType, tokenVersion, date);
        } else if (tokenType.toLowerAscii() == TOKEN_TYPE_RESOURCE) {
            signature = check encoding:encodeUriComponent(keyToken, UTF8_URL_ENCODING);
        } else {
            return prepareUserError(NULL_RESOURCE_TYPE_ERROR);
        }
        if (signature is string) {
            request.setHeader(AUTHORIZATION_HEADER, signature);
        } else {
            return prepareModuleError(NULL_AUTHORIZATION_SIGNATURE_ERROR);
        }
    } else {
        return prepareModuleError(NULL_DATE_ERROR);
    }
}

//  Maps the parameters which are needed for the creation of authorization signature to HeaderParameters type.
// 
//  + httpVerb - HTTP verb of the relevent request.
//  + url - The endpoint to which the request call is made.
//  + return - An instance of record type HeaderParameters.
//
isolated function mapParametersToHeaderType(string httpVerb, string url) returns HeaderParameters {
    HeaderParameters params = {};
    params.verb = httpVerb;
    params.resourceType = getResourceType(url);
    params.resourceId = getResourceId(url);
    return params;
}

//  Get the current time in the specific format.
//  
//  + return - If successful, returns string representing UTC date and time 
//          (in "HTTP-date" format as defined by RFC 7231 Date/Time Formats). Else returns error.
//
isolated function getTime() returns string? {
    time:Time time1 = time:currentTime();
    var timeWithZone = time:toTimeZone(time1, GMT_ZONE);
    if (timeWithZone is time:Time) {
        string timeString = checkpanic time:format(timeWithZone, TIME_ZONE_FORMAT);
        return timeString;
    } else {
        log:printError(TIME_STRING_ERROR);
    }    
}

//  To construct the hashed token signature for a token to set  'Authorization' header.
//  
//  + verb - HTTP verb, such as GET, POST, or PUT
//  + resourceType - identifies the type of resource that the request is for, Eg. "dbs", "colls", "docs"
//  + resourceId -dentity property of the resource that the request is directed at
//  + keyToken - master or resource token
//  + tokenType - denotes the type of token: master or resource.
//  + tokenVersion - denotes the version of the token, currently 1.0.
//  + date - current GMT date and time
//  + return - If successful, returns string which is the  hashed token signature. Else returns () or error.
// 
isolated function generateMasterTokenSignature(string verb, string resourceType, string resourceId, string keyToken, 
        string tokenType, string tokenVersion, string date) returns string?|error {
    string payload = verb.toLowerAscii() + NEW_LINE + resourceType.toLowerAscii() + NEW_LINE + resourceId + NEW_LINE + 
    date.toLowerAscii() + NEW_LINE + EMPTY_STRING + NEW_LINE;
    byte[] decoded = check array:fromBase64(keyToken); 
    byte[] digest = crypto:hmacSha256(payload.toBytes(), decoded);
    string signature = array:toBase64(digest); 
    string? authorization = check encoding:encodeUriComponent(string `type=${tokenType}&ver=${tokenVersion}&sig=${signature}`, "UTF-8");
    return authorization;      
}

//  Set the optional header related to throughput options.
//  
//  + request - http:Request to set the header
//  + throughputOption - Optional. Throughput parameter of type int or json.
//  + return - If successful, returns same http:Request with newly appended headers. Else returns error.
//
isolated function setThroughputOrAutopilotHeader(http:Request request, (int|json)? throughputOption = ()) returns error? {
    if (throughputOption is int) {
        if (throughputOption >= MIN_REQUEST_UNITS) {
            request.setHeader(THROUGHPUT_HEADER, throughputOption.toString());
        } else {
            return prepareUserError(MINIMUM_MANUAL_THROUGHPUT_ERROR);
        }
    } else if (throughputOption != ()) {
        request.setHeader(AUTOPILET_THROUGHPUT_HEADER, throughputOption.toString());
    } else {
        return;
    }
}

//  Set the optional header related to partitionkey value.
//  
//  + request - http:Request to set the header
//  + partitionKey - the array containing the value of the partition key
//  + return - If successful, returns same http:Request with newly appended headers. Else returns error.
//
isolated function setPartitionKeyHeader(http:Request request, any? partitionKey) {
    if (partitionKey is ()) {
        return;
    }
    request.setHeader(PARTITION_KEY_HEADER, string `[${partitionKey.toString()}]`);
}

//  Set the required headers related to query operations.
//  
//  + request - http:Request to set the header
//  + return - If successful, returns same http:Request with newly appended headers. Else returns error.
//
isolated function setHeadersForQuery(http:Request request) {
    var header = request.setContentType(CONTENT_TYPE_QUERY);
    request.setHeader(ISQUERY_HEADER, true.toString());
}

//  Set the optional headers to the HTTP request.
//  
//  + request - http:Request to set the header
//  + requestOptions - object of type RequestHeaderOptions containing the values for optional headers
//  + return - If successful, returns same http:Request with newly appended headers. Else returns error.
//
isolated function setRequestOptions(http:Request request, (DocumentCreateOptions|DocumentReplaceOptions|DocumentGetOptions|
        DocumentListOptions|ResourceReadOptions|ResourceQueryOptions|ResourceDeleteOptions)? requestOptions) returns error? {
    if (requestOptions?.indexingDirective != ()) {
        if (requestOptions?.indexingDirective == INDEXING_TYPE_INCLUDE || requestOptions?.indexingDirective == INDEXING_TYPE_EXCLUDE) {
            request.setHeader(INDEXING_DIRECTIVE_HEADER, <string>requestOptions?.indexingDirective);
        } else {
            return prepareModuleError(INDEXING_DIRECTIVE_ERROR);
        }
    }
    if (requestOptions?.consistancyLevel != ()) {
        if (requestOptions?.consistancyLevel == CONSISTANCY_LEVEL_STRONG || requestOptions?.consistancyLevel == 
        CONSISTANCY_LEVEL_BOUNDED || requestOptions?.consistancyLevel == CONSISTANCY_LEVEL_SESSION || requestOptions?.
        consistancyLevel == CONSISTANCY_LEVEL_EVENTUAL) {
            request.setHeader(CONSISTANCY_LEVEL_HEADER, requestOptions?.consistancyLevel.toString());
        } else {
            return prepareModuleError(CONSISTANCY_LEVEL_ERROR);
        }
    }
    if (requestOptions?.sessionToken != ()) {
        request.setHeader(SESSION_TOKEN_HEADER, requestOptions?.sessionToken.toString());
    }
    if (requestOptions?.changeFeedOption != ()) {
        request.setHeader(A_IM_HEADER, requestOptions?.changeFeedOption.toString());
    }
    if (requestOptions?.ifNoneMatchEtag != ()) {
        request.setHeader(NON_MATCH_HEADER, requestOptions?.ifNoneMatchEtag.toString());
    }
    if (requestOptions?.partitionKeyRangeId != ()) {
        request.setHeader(PARTITIONKEY_RANGE_HEADER, requestOptions?.partitionKeyRangeId.toString());
    }
    if (requestOptions?.ifMatchEtag != ()) {
        request.setHeader(IF_MATCH_HEADER, requestOptions?.ifMatchEtag.toString());
    }
    if (requestOptions?.enableCrossPartition == true) {
        request.setHeader(IS_ENABLE_CROSS_PARTITION_HEADER, requestOptions?.enableCrossPartition.toString());
    }
    if (requestOptions?.isUpsertRequest == true) {
        request.setHeader(IS_UPSERT_HEADER, requestOptions?.isUpsertRequest.toString());
    }
}

//  Set the optional header specifying time to live.
//  
//  + request - http:Request to set the header
//  + validationPeriod - the integer specifying the time to live value for a permission token
//  + return - If successful, returns same http:Request with newly appended headers. Else returns error.
//
isolated function setExpiryHeader(http:Request request, int validationPeriod) returns error? {
    if (validationPeriod >= MIN_TIME_TO_LIVE && validationPeriod <= MAX_TIME_TO_LIVE) {
        request.setHeader(EXPIRY_HEADER, validationPeriod.toString());
    } else {
        return prepareUserError(VALIDITY_PERIOD_ERROR);
    }
}

//  Map the json payload and necessary header values returend from a response to a tuple.
//  
//  + httpResponse - the http:Response or http:ClientError returned form the HTTP request
//  + return - returns a tuple of type [json, ResponseMetadata] if sucessful else, returns error
//
isolated function mapResponseToTuple(http:Response httpResponse) returns @tainted [json, 
        ResponseMetadata]|error {
    json responseBody = check handleResponse(httpResponse);
    ResponseMetadata responseHeaders = check mapResponseHeadersToHeadersRecord(httpResponse);
    return [responseBody, responseHeaders];
}

//  Map the json payload and necessary header values returend from a response to a tuple.
//  
//  + httpResponse - the http:Response or http:ClientError returned form the HTTP request
//  + return - returns a tuple of type [json, ResponseMetadata] if sucessful else, returns error
//
isolated function mapCreationResponseToTuple(http:Response httpResponse) returns @tainted [boolean, 
        ResponseMetadata]|error {
    boolean responseBody = check handleCreationResponse(httpResponse);
    ResponseMetadata responseHeaders = check mapResponseHeadersToHeadersRecord(httpResponse);
    return [responseBody, responseHeaders];
}

//  Handle sucess or error reponses to requests and extract the json payload.
//  
//  + httpResponse - http:Response or http:ClientError returned from an http:Request
//  + return - If successful, returns json. Else returns error. 
//
isolated function handleResponse(http:Response httpResponse) returns @tainted json|error {
    if (httpResponse.statusCode == http:STATUS_NO_CONTENT) {
        //If status 204, then no response body. So returns empty json.
        return {};
    }
    var jsonResponse = httpResponse.getJsonPayload();
    if (jsonResponse is json) {
        if (httpResponse.statusCode == http:STATUS_OK || httpResponse.statusCode == http:STATUS_CREATED) {
            //If status is 200 or 201, request is successful. Returns resulting payload.
            return jsonResponse;
            
        } else {
            string message = jsonResponse.message.toString();
            return prepareModuleError(message, (), httpResponse.statusCode);
        }
    } else {
        return prepareModuleError(JSON_PAYLOAD_ACCESS_ERROR, jsonResponse);
    }
}

//  Handle sucess or error reponses to requests and extract the json payload.
//  
//  + httpResponse - http:Response or http:ClientError returned from an http:Request
//  + return - If successful, returns json. Else returns error. 
//
isolated function handleCreationResponse(http:Response httpResponse) returns @tainted boolean|error {
    var jsonResponse = httpResponse.getJsonPayload();
    if (jsonResponse is json) {
        if (httpResponse.statusCode == http:STATUS_OK || httpResponse.statusCode == http:STATUS_CREATED) {
            //If status is 200 or 201, request is successful. Returns resulting payload.
            return true;
            
        } else {
            string message = jsonResponse.message.toString();
            return prepareModuleError(message, (), httpResponse.statusCode);
        }
    } else {
        return prepareModuleError(JSON_PAYLOAD_ACCESS_ERROR, jsonResponse);
    }
}

//  Get the http:Response and extract the headers to the record type ResponseMetadata
//  
//  + httpResponse - http:Response or http:ClientError returned from an http:Request
//  + return - If successful, returns record type ResponseMetadata. Else returns error.
//
isolated function mapResponseHeadersToHeadersRecord(http:Response httpResponse) returns @tainted ResponseMetadata|error {
    ResponseMetadata responseHeaders = {};
    responseHeaders.continuationHeader = getHeaderIfExist(httpResponse, CONTINUATION_HEADER) == "" ? () : 
            getHeaderIfExist(httpResponse, CONTINUATION_HEADER);
    responseHeaders.sessionToken = getHeaderIfExist(httpResponse, SESSION_TOKEN_HEADER);
    responseHeaders.requestCharge = getHeaderIfExist(httpResponse, REQUEST_CHARGE_HEADER);
    responseHeaders.resourceUsage = getHeaderIfExist(httpResponse, RESOURCE_USAGE_HEADER);
    responseHeaders.etag = getHeaderIfExist(httpResponse, ETAG_HEADER);
    responseHeaders.date = getHeaderIfExist(httpResponse, RESPONSE_DATE_HEADER);
    return responseHeaders;
}

//  Convert json string values to int
//  
//  + httpResponse - http:Response returned from an http:RequestheaderName
//  + headerName - name of the header
//  + return - int value of specified json
//
isolated function getHeaderIfExist(http:Response httpResponse, string headerName) returns @tainted string {
    string headerValue = "";
    if (httpResponse.hasHeader(headerName)) {
        headerValue = httpResponse.getHeader(headerName);
    } 
    return headerValue;
} 

//  Get a stream of json documents which is returned as query results
//  
//  + azureCosmosClient - 
//  + path - 
//  + request - n
//  + array - 
//  + maxItemCount - 
//  + continuationHeader - 
// 
function getQueryResults(http:Client azureCosmosClient, string path, http:Request request, @tainted json[] array, 
        int? maxItemCount = (), string? continuationHeader = ()) returns @tainted stream<json>|error {
    if (continuationHeader is string) {
        request.setHeader(CONTINUATION_HEADER, continuationHeader);
    }
    if (maxItemCount is int) { // this is max item count per page value
        request.setHeader(MAX_ITEM_COUNT_HEADER, maxItemCount.toString());
    }
    http:Response response = <http:Response> check azureCosmosClient->post(path, request);
    var [payload, responseHeaders] = check mapResponseToTuple(response);

    if (payload.Documents is json) {
        appendNewJsonArray(array, <json[]>payload.Documents);
        stream<json> documentStream = (<@untainted>array).toStream();
        if (responseHeaders?.continuationHeader != () && maxItemCount is ()) {
            documentStream = check getQueryResults(azureCosmosClient, path, request, array, (), responseHeaders?.continuationHeader);
        }
        return documentStream;
    } else if (payload.Offers is json) {
        appendNewJsonArray(array, <json[]>payload.Offers);
        stream<json> offerStream = (<@untainted>array).toStream();
        if (responseHeaders?.continuationHeader != () && maxItemCount is ()) {
            offerStream = check getQueryResults(azureCosmosClient, path, request, array, (), responseHeaders?.continuationHeader);
        }
        return offerStream;
    }
    else {
        return prepareModuleError(INVALID_RESPONSE_PAYLOAD_ERROR);
    }
}

isolated function appendNewJsonArray(json[] array, json[] newArray) {
    int i = array.length();
    foreach json element in newArray {
        array[i] = element;
        i = i + 1;
    }
}

function retriveStream(http:Client azureCosmosClient, string path, http:Request request,@tainted record{}[] array, 
        int? maxItemCount = (), @tainted string? continuationHeader = ()) returns @tainted stream<record{}>|error {
    if (continuationHeader is string) {
        request.setHeader(CONTINUATION_HEADER, continuationHeader);
    }

    http:Response response = <http:Response> check azureCosmosClient->get(path, request);
    var [payload, headers] = check mapResponseToTuple(response);

    stream<record{}> finalStream = check createStream(azureCosmosClient, path, request, array, payload, 
            headers?.continuationHeader, maxItemCount);
    return finalStream;
}

function createStream(http:Client azureCosmosClient, string path, http:Request request, @tainted record{}[] array, 
        json payload, @tainted string? continuationHeader = (), int? maxItemCount = ()) returns @tainted stream<record{}>|error {
    var arrayType = typeof array;
    record{}[] finalArray = array;

    if (arrayType is typedesc<Offer[]>) {
        if (payload.Offers is json) {
            finalArray = ConvertToOfferArray(<Offer[]>array, <json[]>payload.Offers);
        } else {
            return prepareModuleError(INVALID_RESPONSE_PAYLOAD_ERROR);
        }
    } else if (arrayType is typedesc<Document[]>) {
        if (payload.Documents is json) {
            finalArray = convertToDocumentArray(<Document[]>array, <json[]>payload.Documents);
        } else {
            return prepareModuleError(INVALID_RESPONSE_PAYLOAD_ERROR);
        }
    } else if (arrayType is typedesc<Database[]>) {
        if (payload.Databases is json) {
            finalArray = convertToDatabaseArray(<Database[]>array, <json[]>payload.Databases);
        } else {
            return prepareModuleError(INVALID_RESPONSE_PAYLOAD_ERROR);
        }
    } else if (arrayType is typedesc<Container[]>) {
        if (payload.DocumentCollections is json) {
            finalArray = convertToContainerArray(<Container[]>array, <json[]>payload.DocumentCollections);
        } else {
            return prepareModuleError(INVALID_RESPONSE_PAYLOAD_ERROR);
        }
    } else if (arrayType is typedesc<StoredProcedure[]>) {
        if (payload.StoredProcedures is json) {
            finalArray = convertToStoredProcedureArray(<StoredProcedure[]>array, <json[]>payload.StoredProcedures);
        } else {
            return prepareModuleError(INVALID_RESPONSE_PAYLOAD_ERROR);
        }     
    } else if (arrayType is typedesc<UserDefinedFunction[]>) {
        if (payload.UserDefinedFunctions is json) {
            finalArray = convertsToUserDefinedFunctionArray(<UserDefinedFunction[]>array, <json[]>payload.UserDefinedFunctions);
        } else {
            return prepareModuleError(INVALID_RESPONSE_PAYLOAD_ERROR);
        }
    } else if (arrayType is typedesc<Trigger[]>) {
        if (payload.Triggers is json) {
            finalArray = convertToTriggerArray(<Trigger[]>array, <json[]>payload.Triggers);
        } else {
            return prepareModuleError(INVALID_RESPONSE_PAYLOAD_ERROR);
        }
    } else if (arrayType is typedesc<User[]>) {
        if (payload.Users is json) {
            finalArray = convertToUserArray(<User[]>array, <json[]>payload.Users);
        } else {
            return prepareModuleError(INVALID_RESPONSE_PAYLOAD_ERROR);
        }
    } else if (arrayType is typedesc<Permission[]>) {
        if (payload.Permissions is json) {
            finalArray = convertToPermissionArray(<Permission[]>array, <json[]>payload.Permissions);
        } else {
            return prepareModuleError(INVALID_RESPONSE_PAYLOAD_ERROR);
        }
    } else if (arrayType is typedesc<PartitionKeyRange[]>) {
        if (payload.PartitionKeyRanges is json) {
            finalArray = convertToPartitionKeyRangeArray(<json[]>payload.PartitionKeyRanges);
        } else {
            return prepareModuleError(INVALID_RESPONSE_PAYLOAD_ERROR);
        }
    } else {
        return prepareModuleError(INVALID_STREAM_TYPE);
    }

    stream<record{}> newStream = (<@untainted>finalArray).toStream();
    if (continuationHeader != () && maxItemCount is ()) {
        newStream = check retriveStream(azureCosmosClient, path, request, <@untainted>finalArray, (), continuationHeader);
    }
    return newStream;
}

//  Convert json string values to boolean.
//  
//  + value - json value which has reprsents boolean value
//  + return - boolean value of specified json
//
isolated function convertToBoolean(json|error value) returns boolean {
    if (value is json) {
        boolean|error result = 'boolean:fromString(value.toString());
        if (result is boolean) {
            return result;
        }
    }
    return false;
}

//  Convert json string values to int
//  
//  + value - json value which has reprsents int value
//  + return - int value of specified json
//
isolated function convertToInt(json|error value) returns int {
    if (value is json) {
        int|error result = 'int:fromString(value.toString());
        if (result is int) {
            return result;
        }
    }
    return 0;
}

# Create a random UUID removing the unnecessary hyphens which will interrupt querying opearations.
# 
# + return - A string UUID without hyphens
public function createRandomUUIDWithoutHyphens() returns string {
    string? stringUUID = java:toString(createRandomUUID());
    if (stringUUID is string) {
        stringUUID = stringutils:replace(stringUUID, "-", "");
        return stringUUID;
    } else {
        return "";
    }
}

function createRandomUUID() returns handle = @java:Method {
    name: "randomUUID",
    'class: "java.util.UUID"
} external;
