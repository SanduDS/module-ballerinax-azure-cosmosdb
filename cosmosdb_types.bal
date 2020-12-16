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

import ballerina/http;

# Represents configuration parameters to create Azure Cosmos DB client.
# 
# + baseUrl - Base URL of the Azure Cosmos DB account.
# + keyOrResourceToken - The token usesd to make the request call.
# + tokenType - The type of token usesd to make the request call "master" or "resource". 
# + tokenVersion - The version of the token.
# + secureSocketConfig - The secure socket config.
public type AzureCosmosConfiguration record {|
    string baseUrl;
    string keyOrResourceToken;
    string tokenType;
    string tokenVersion;
    http:ClientSecureSocket? secureSocketConfig;
|};

# Represents resource properties which are needed to make the request call.
# 
# + databaseId - Id of the database which the request is made.
# + containerId - Id of the container which the request is made.
public type ResourceProperties record {|
    string databaseId = "";
    string containerId = "";
|};

# Represents the common elements which are returned inside json reponse body.
# 
# + resourceId - Resource id (_rid), a unique identifier which is used internally for placement and navigation of the resource.
# + selfReference - Self reference (_self) unique addressable URI for the resource.
# + timeStamp - Timestamp (_ts) specifies the last updated timestamp of the resource.
# + eTag - Representing the resource etag (_etag) required for optimistic concurrency control. 
public type Common record {|
    string resourceId?;
    string selfReference?;
    string timeStamp?;
    string eTag?;
|};

# Represents the optional request headers which can be set in a request.
# 
# + isUpsertRequest - A boolean value which specify if the request is an upsert request.
# + indexingDirective - The option whether to include the document in the index. Allowed values are "Include" or "Exclude".
# + consistancyLevel - The consistancy level override. Allowed values are "Strong", "Bounded", "Sesssion" or "Eventual".
# + sessionToken - Echo the latest read value of sessionTokenHeader to aquire session level consistancy. 
# + changeFeedOption - Must be set to "Incremental feed" or omitted otherwise.
# + ifNoneMatch - Specify "*" to return all new changes, "<eTag>" to return changes made sice that timestamp or otherwise omitted.
# + partitionKeyRangeId - The partition key range ID for reading data.
# + enableCrossPartition -  Boolean value specifying whether to allow cross partitioning.
# + ifMatch - Used to make operation conditional for optimistic concurrency. 
public type RequestHeaderOptions record {|
    boolean? isUpsertRequest = ();
    string? indexingDirective = ();
    string? consistancyLevel = ();
    string? sessionToken = ();
    string? changeFeedOption = ();
    string? ifNoneMatch = ();
    string? partitionKeyRangeId = ();
    boolean? enableCrossPartition = ();
    string? ifMatch = ();
|};

# Represents the response headers which is returned.
# 
# + continuationHeader - Token returned for queries and read-feed operations if there are more results to be read.
# + sessionTokenHeader - Session token of the request.
# + requestChargeHeader - This is the number of normalized requests a.k.a. request units (RU) for the operation.
# + resourceUsageHeader - Current usage count of a resource in an account.  
# + itemCountHeader - Number of items returned for a query or read-feed request.
# + etagHeader - Resource etag for the resource retrieved same as eTag in the response. 
# + dateHeader - Date time of the response operation.
public type Headers record {|
    string? continuationHeader = ();
    string? sessionTokenHeader = ();
    string? requestChargeHeader = ();
    string? resourceUsageHeader = ();
    string? itemCountHeader = ();
    string? etagHeader = ();
    string? dateHeader = ();
|};

# Represents the elements representing information about a database.
# 
# + id - User generated unique ID for the database. 
# + collections - Addressable path of the collections resource.
# + users - Addressable path of the users resource.
public type Database record {|
    string id = "";
    *Common;
    string collections?;
    string users?;
    Headers?...;
|};

# Represents the elements representing information about a collection.
# 
# + id - User generated unique ID for the container.
# + indexingPolicy - Object of type IndexingPolicy. 
# + partitionKey - Object of type PartitionKey.
# + collections - Addressable path of the collections resource.
# + storedProcedures - Addressable path of the stored procedures resource.
# + triggers - Addressable path of the triggers resource.
# + userDefinedFunctions - Addressable path of the user-defined functions resource.
# + conflicts - Addressable path of the conflicts resource. 
# + allowMaterializedViews - Representing whether to allow materialized views.
public type Container record {|
    string id = "";
    *Common;
    IndexingPolicy indexingPolicy?;
    PartitionKey partitionKey = {};
    string collections?;
    string storedProcedures?;
    string triggers?;
    string userDefinedFunctions?;
    string conflicts?;
    boolean allowMaterializedViews?;
    Headers?...;
|};

# Represents the elements representing information about a document.
# 
# + id - User generated unique ID for the document. 
# + documentBody - BSON document.
# + partitionKey - Array containing the value for the selected partition key.
# + attachments - Addressable path for the attachments resource.
public type Document record {|
    string id = "";
    *Common;
    json documentBody = {};
    any[]? partitionKey = ();
    string attachments?;
    Headers?...;
|};

# Represents the elements representing information about a document.
# 
# + indexingMode - Mode of indexing.
# + automatic - Whether indexing is automatic.
# + includedPaths - Array of type IncludedPath representing included paths.
# + excludedPaths - Array of type IncludedPath representing excluded paths.
public type IndexingPolicy record {|
    string indexingMode = "";
    boolean automatic?;
    IncludedPath[] includedPaths?;
    IncludedPath[] excludedPaths?;
|};

# Represent the structure of included path type.
# 
# + path - Path for which the indexing behavior applies to.
# + indexes - Array of type Index representing index values.
public type IncludedPath record {|
    string path = "";
    Index[] indexes?;
|};

# Represent the structure of excluded path type.
# 
# + path - Path that is excluded from indexing. 
public type ExcludedPath record {|
    string path?;
|};

# Represent the record type with elements represent an index. 
# 
# + kind - Type of index. Can be "Hash", "Range" or "Spatial"
# + dataType - Datatype for which the indexing behavior is applied to. Can be "String", "Number", "Point", "Polygon" 
#   or "LineString"
# + precision - Precision of the index. Can be either set to -1 for maximum precision or between 1-8 for Number, 
#   and 1-100 for String. Not applicable for Point, Polygon, and LineString data types.
public type Index record {|
    string kind = "";
    string dataType = "";
    int precision?;
|};

# Represent the record type with elements represent a partition key.
# 
# + paths - Array of paths using which data within the collection can be partitioned. The array must contain only a 
#   single value.
# + kind - Algorithm used for partitioning. Only Hash is supported.
# + keyVersion - Version of partition key.
public type PartitionKey record {|
    string[] paths = [];
    string kind = "Hash";
    int keyVersion = 1;
|};

# Reprsnets the record type with necessary paramaters to create partition key range.
# 
# + id - ID for the partition key range.
# + minInclusive - Maximum partition key hash value for the partition key range. 
# + maxExclusive - Minimum partition key hash value for the partition key range. 
# + status - 
public type PartitionKeyRange record {|
    string id = "";
    *Common;
    string minInclusive = "";
    string maxExclusive = "";
    string status = "";
    Headers?...;
|};

# Represent the record type with elements represent a stored procedure.
# 
# + id - User generated unique ID for the stored procedure. 
# + body - Body of the stored procedure.
public type StoredProcedure record {|
    string id = "";
    *Common;
    string body = "";
    Headers?...;
|};

public type UserDefinedFunction record {|
    string id = "";
    *Common;
    string body = "";    
    Headers?...;
|};

# Represent the record type with elements represent a trigger.
# 
# + triggerOperation - Type of operation that invokes the trigger. Can be "All", "Create", "Replace" or "Delete".
# + triggerType - When the trigger is fired, "Pre" or "Post".
public type Trigger record {|
    *StoredProcedure;
    string triggerOperation = "";
    string triggerType = "";
    Headers?...;
|};

# Represent the record type with elements represent a user.
# 
# + id - User generated unique ID for the user. 
# + permissions - Addressable path of the permissions resource.
public type User record {|
    string id = "";
    *Common;
    string permissions?;
    Headers?...;
|};

# Represent the record type with elements represent a permission.
# 
# + id - User generated unique ID for the permission.
# + permissionMode - Access mode for the resource, "All" or "Read".
# + resourcePath - Full addressable path of the resource associated with the permission.
# + validityPeriod - Validity period of the resource token.
# + token - System generated resource token for the particular resource and user.
public type Permission record {|
    string id = "";
    *Common;
    string permissionMode = "";
    string resourcePath = "";
    int validityPeriod?;
    string token?;
    Headers?...;
|};

# Represent the record type with elements represent an offer.
# 
# + id - User generated unique ID for the offer.
# + offerVersion - Offer version, This value can be V1 for pre-defined throughput levels and V2 for user-defined throughput levels.
# + offerType - Performance level for V1 offer version, allows S1,S2 and S3.
# + content - Information about the offer.
# + offerResourceId - A property which is automatically done, associated to the resource ID(_rid).
# + resourceSelfLink - The self-link of the collection.
public type Offer record {|
    string id = "";
    *Common;
    string offerVersion = "";
    string? offerType?; 
    json content = {};
    string offerResourceId = "";
    string resourceSelfLink = "";
    Headers?...;
|};

# Represent the record type with options represent for throughput.
# 
# + throughput - Manual throughput value which must be more than 400RU/s.
# + maxThroughput - Autoscaling throughout which is represented as a json object.
public type ThroughputProperties record {
    int? throughput = ();
    json? maxThroughput = ();
};

# Represent the record type with the necessary paramateres for creation of authorization signature.
# 
# + verb - HTTP verb of the request call.
# + apiVersion - Version of the API as given by the user.
# + resourceType - Resource type, the relevent request targetted to.
# + resourceId - Resource ID, the relevent request targetted to.
public type HeaderParameters record {|
    string verb = "";
    string apiVersion = API_VERSION;
    string resourceType = "";
    string resourceId = "";
|};

public type AzureError distinct error;

type JsonMap map<json>;

# Represents the record type which contain necessary elements for a query.
# 
# + query - The SQL query represented as string.
# + parameters - Parameters of the query if exists.
public type Query record {|
    string query = "";
    QueryParameter[]? parameters = [];
|};

# Represnent the paramaters related to query.
# 
# + name - Name of the paramater.
# + value - Value of the paramater.
public type QueryParameter record {|
    string name = "";
    string value = "";
|};