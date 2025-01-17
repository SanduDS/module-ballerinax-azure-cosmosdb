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

import ballerina/test;
import ballerina/config;
import ballerina/system;
import ballerina/log;
import ballerina/runtime;

AzureCosmosConfiguration config = {
    baseUrl : config:getAsString("BASE_URL"), 
    keyOrResourceToken : config:getAsString("KEY_OR_RESOURCE_TOKEN"), 
    tokenType : config:getAsString("TOKEN_TYPE"), 
    tokenVersion : config:getAsString("TOKEN_VERSION")
};

Client AzureCosmosClient = new(config);

Database database = {};
Database manual = {};
Database auto = {};
Database ifexist = {};
Container container = {};
Document document = {};
StoredProcedure storedPrcedure = {};
UserDefinedFunction udf = {};
Trigger trigger = {};
User test_user = {};
Permission permission = {};

@test:Config{
    groups: ["database"]
}
function test_createDatabase(){
    log:print("ACTION : createDatabase()");

    var uuid = createRandomUUIDBallerina();
    string createDatabaseId = string `database_${uuid.toString()}`;

    var result = AzureCosmosClient->createDatabase(createDatabaseId);
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        database = <@untainted>result;
    }
}

@test:Config{
    groups: ["database"]
}
function test_createDatabaseUsingInvalidId(){
    log:print("ACTION : createDatabaseUsingInvalidId()");

    string createDatabaseId = "";

    var result = AzureCosmosClient->createDatabase(createDatabaseId);
    if (result is Database){
        test:assertFail(msg = "Database created with  '' id value");
    } else {
        var output = "";
    }
}

@test:Config{
    groups: ["database"], 
    dependsOn: ["test_createDatabase"]
}
function test_createDatabaseIfNotExist(){
    log:print("ACTION : createDatabaseIfNotExist()");

    var uuid = createRandomUUIDBallerina();
    string createDatabaseId = string `databasee_${uuid.toString()}`;

    var result = AzureCosmosClient->createDatabaseIfNotExist(createDatabaseId);
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        ifexist = <@untainted><Database> result;
    }
}

@test:Config{
    groups: ["database"], 
    dependsOn: ["test_createDatabase"]
}
function test_createDatabaseIfExist(){
    log:print("ACTION : createDatabaseIfExist()");

    var uuid = createRandomUUIDBallerina();
    string createDatabaseId = database.id;

    var result = AzureCosmosClient->createDatabaseIfNotExist(createDatabaseId);
    if (result is Database){
        test:assertFail(msg = "Database with non unique id is created");
    } else {
        var output = "";
    }
}

@test:Config{
    groups: ["database"],
    enable: false
}
function test_createDatabaseWithManualThroughput(){
    log:print("ACTION : createDatabaseWithManualThroughput()");

    var uuid = createRandomUUIDBallerina();
    string createDatabaseManualId = string `databasem_${uuid.toString()}`;
    int throughput = 1000;

    var result = AzureCosmosClient->createDatabase(createDatabaseManualId,  throughput);
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        manual = <@untainted>result;
    }
}

@test:Config{
    groups: ["database"]
}
function test_createDatabaseWithInvalidManualThroughput(){
    log:print("ACTION : createDatabaseWithInvalidManualThroughput()");

    var uuid = createRandomUUIDBallerina();
    string createDatabaseManualId = string `databasem_${uuid.toString()}`;
    int throughput = 40;

    var result = AzureCosmosClient->createDatabase(createDatabaseManualId,  throughput);
    if (result is Database){
        test:assertFail(msg = "Database created without validating user input");
    } else {
        var output = "";
    }
}

@test:Config{
    groups: ["database"],
    enable: false
}
function test_createDBWithAutoscalingThroughput(){
    log:print("ACTION : createDBWithAutoscalingThroughput()");

    var uuid = createRandomUUIDBallerina();
    string createDatabaseAutoId = string `databasea_${uuid.toString()}`;
    json maxThroughput = {"maxThroughput": 4000};

    var result = AzureCosmosClient->createDatabase(createDatabaseAutoId,  maxThroughput);
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        auto = <@untainted> result;
    }
}

@test:Config{
    groups: ["database"]
}
function test_listAllDatabases(){
    log:print("ACTION : listAllDatabases()");

    var result = AzureCosmosClient->listDatabases(6);
    if (result is stream<Database>){
        var database = result.next();
    } else {
        test:assertFail(msg = result.message());
    }
}

@test:Config{
    groups: ["database"], 
    dependsOn: ["test_createDatabase"]
}
function test_listOneDatabase(){
    log:print("ACTION : listOneDatabase()");

    var result = AzureCosmosClient->getDatabase(database.id);
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    }
}

@test:Config{
    groups: ["database"], 
    dependsOn: [
        "test_createDatabase", 
        "test_createDatabaseIfNotExist", 
        "test_listOneDatabase", 
        "test_createDatabase", 
        "test_getAllContainers", 
        "test_GetPartitionKeyRanges", 
        "test_getDocumentListWithRequestOptions", 
        "test_createDocumentWithRequestOptions", 
        "test_getDocumentList", 
        "test_createCollectionWithManualThroughputAndIndexingPolicy", 
        "test_deleteDocument", 
        "test_deleteOneStoredProcedure", 
        "test_getAllStoredProcedures", 
        "test_listUsers", 
        "test_deleteUDF", 
        "test_deleteTrigger", 
        "test_deleteUser", 
        "test_createContainerIfNotExist", 
        "test_deleteContainer", 
        "test_createPermissionWithTTL", 
        "test_getCollection_Resource_Token"
    ]
}
function test_deleteDatabase(){
    log:print("ACTION : deleteDatabase()");

    var result1 = AzureCosmosClient->deleteDatabase(database.id);
    var result2 = AzureCosmosClient->deleteDatabase(manual.id);
    var result3 = AzureCosmosClient->deleteDatabase(auto.id);
    var result4 = AzureCosmosClient->deleteDatabase(ifexist.id);
    if (result1 is error){
        test:assertFail(msg = result1.message());
    } else {
        var output = "";
    }
}

@test:Config{
    groups: ["container"], 
    dependsOn: ["test_createDatabase"]
}
function test_createContainer(){
    log:print("ACTION : createContainer()");

    var uuid = createRandomUUIDBallerina();
    string databaseId = database.id;
    string containerId = string `container_${uuid.toString()}`;
    PartitionKey pk = {
        paths: ["/AccountNumber"], 
        keyVersion: 2
    };
    var result = AzureCosmosClient->createContainer(databaseId, containerId, pk);
    if (result is Container){
        container = <@untainted>result;
    } else {
        test:assertFail(msg = result.message());
    } 
}

@test:Config{
    groups: ["container"], 
    dependsOn: ["test_createContainer"]
}
function test_createCollectionWithManualThroughputAndIndexingPolicy(){
    log:print("ACTION : createCollectionWithManualThroughputAndIndexingPolicy()");
    
    var uuid = createRandomUUIDBallerina();
    string databaseId = database.id;
    string containerId = string `container_${uuid.toString()}`;
    IndexingPolicy ip = {
        indexingMode : "consistent", 
        automatic : true, 
        includedPaths : [{
            path : "/*", 
            indexes : [{
                dataType: "String",  
                precision: -1,  
                kind: "Range"  
            }]
        }]
    };
    int throughput = 600;
    PartitionKey pk = {
        paths: ["/AccountNumber"], 
        kind : "Hash", 
        keyVersion : 2
    };
    
    var result = AzureCosmosClient->createContainer(databaseId, containerId, pk, ip, throughput);
    if (result is Container){
        var output = "";
    } else {
        test:assertFail(msg = result.message());
    } 
}
 
@test:Config{
    groups: ["container"], 
    dependsOn: ["test_createDatabase",  "test_getOneContainer"]
}
function test_createContainerIfNotExist(){
    log:print("ACTION : createContainerIfNotExist()");

    var uuid = createRandomUUIDBallerina();
    string databaseId = database.id;
    string containerId = string `container_${uuid.toString()}`;
    PartitionKey pk = {
        paths: ["/AccountNumber"], 
        kind :"Hash", 
        keyVersion: 2
    };

    var result = AzureCosmosClient->createContainerIfNotExist(databaseId, containerId, pk);
    if (result is Container?){
        var output = "";
    } else {
        test:assertFail(msg = result.message());
    }
}

@test:Config{
    groups: ["container"], 
    dependsOn: ["test_createContainer"]
}
function test_getOneContainer(){
    log:print("ACTION : getOneContainer()");

    string databaseId = database.id;
    string containerId = container.id;

    var result = AzureCosmosClient->getContainer(databaseId, containerId);
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    }
}

@test:Config{
    groups: ["container"], 
    dependsOn: ["test_createDatabase"]
}
function test_getAllContainers(){
    log:print("ACTION : getAllContainers()");

    var result = AzureCosmosClient->listContainers(database.id);
    if (result is stream<Container>){
        var container = result.next();
    } else {
        test:assertFail(msg = result.message());
    }
}

@test:Config{
    groups: ["container"], 
    dependsOn: [
        "test_getOneContainer", 
        "test_GetPartitionKeyRanges", 
        "test_getDocumentList", 
        "test_deleteDocument", 
        "test_queryDocuments", 
        "test_queryDocumentsWithRequestOptions", 
        "test_getAllStoredProcedures", 
        "test_deleteOneStoredProcedure", 
        "test_listAllUDF", 
        "test_deleteUDF", 
        "test_deleteTrigger", 
        "test_GetOneDocumentWithRequestOptions", 
        "test_createDocumentWithRequestOptions", 
        "test_getDocumentListWithRequestOptions", 
        "test_getCollection_Resource_Token",
        "test_getAllContainers"
        //"test_replaceOfferWithOptionalParameter",
        //"test_replaceOffer"
    ]
}
function test_deleteContainer(){
    log:print("ACTION : deleteContainer()");

    string databaseId = database.id;
    string containerId = container.id;

    var result = AzureCosmosClient->deleteContainer(databaseId, containerId);
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    }
}

@test:Config{
    groups: ["partitionKey"], 
    dependsOn: ["test_createContainer"]
}
function test_GetPartitionKeyRanges(){
    log:print("ACTION : GetPartitionKeyRanges()");

    string databaseId = database.id;
    string containerId = container.id;

    var result = AzureCosmosClient->listPartitionKeyRanges(databaseId, containerId);
    if (result is stream<PartitionKeyRange>){
        var partitionKeyRanges = result.next();
    } else {
        test:assertFail(msg = result.message());
    }  
}

@test:Config{
    groups: ["document"], 
    dependsOn: ["test_createContainer"]
}
function test_createDocument(){
    log:print("ACTION : createDocument()");

    var uuid = createRandomUUIDBallerina();
    string databaseId = database.id;
    string containerId = container.id;
    Document createDoc = {
        id: string `document_${uuid.toString()}`, 
        documentBody :{
            "LastName": "keeeeeee",  
        "Parents": [  
            {  
            "FamilyName": null,  
            "FirstName": "Thomas"  
            },  
            {  
            "FamilyName": null,  
            "FirstName": "Mary Kay"  
            }  
        ],  
        "Children": [  
            {  
            "FamilyName": null,  
            "FirstName": "Henriette Thaulow",  
            "Gender": "female",  
            "Grade": 5,  
            "Pets": [  
                {  
                "GivenName": "Fluffy"  
                }  
            ]  
            }  
        ],  
        "Address": {  
            "State": "WA",  
            "County": "King",  
            "City": "Seattle"  
        },  
        "IsRegistered": true, 
        "AccountNumber": 1234
        }, 
        partitionKey : [1234]  
    };

    var result = AzureCosmosClient->createDocument(databaseId, containerId,  createDoc);
    if (result is Document){
        document = <@untainted>result;
    } else {
        test:assertFail(msg = result.message());
    }   
}

@test:Config{
    groups: ["document"], 
    dependsOn: ["test_createContainer"]
}
function test_createDocumentWithRequestOptions(){
    log:print("ACTION : createDocumentWithRequestOptions()");

    var uuid = createRandomUUIDBallerina();
    string databaseId = database.id;
    string containerId = container.id;
    DocumentCreateOptions options = {
        isUpsertRequest : true, 
        indexingDirective : "Include", 
        ifMatchEtag : "hhh"
    };
    Document createDoc = {
        id: string `document_${uuid.toString()}`, 
        documentBody :{
            "LastName": "keeeeeee",  
        "Parents": [  
            {  
            "FamilyName": null,  
            "FirstName": "Thomas"  
            },  
            {  
            "FamilyName": null,  
            "FirstName": "Mary Kay"  
            }  
        ],  
        "Children": [  
            {  
            "FamilyName": null,  
            "FirstName": "Henriette Thaulow",  
            "Gender": "female",  
            "Grade": 5,  
            "Pets": [  
                {  
                "GivenName": "Fluffy"  
                }  
            ]  
            }  
        ],  
        "Address": {  
            "State": "WA",  
            "County": "King",  
            "City": "Seattle"  
        },  
        "IsRegistered": true, 
        "AccountNumber": 1234
        }, 
        partitionKey : [1234]  
    };
    var result = AzureCosmosClient->createDocument(databaseId, containerId,  createDoc,  options);
    if (result is Document){
        document = <@untainted>result;
    } else {
        test:assertFail(msg = result.message());
    }   
}

@test:Config{
    groups: ["document"], 
    dependsOn: ["test_createDocument"]
}
function test_getDocumentList(){
    log:print("ACTION : getDocumentList()");

    string databaseId = database.id;
    string containerId = container.id;

    var result = AzureCosmosClient->getDocumentList(databaseId, containerId);
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
       var singleDocument = result.next();
    }
}

@test:Config{
    groups: ["document"], 
    dependsOn: ["test_createDocument"]
}
function test_getDocumentListWithRequestOptions(){
    log:print("ACTION : getDocumentListWithRequestOptions()");

    string databaseId = database.id;
    string containerId = container.id;

    DocumentListOptions options = {
        consistancyLevel : "Eventual", 
        sessionToken: "tag", 
        ifNoneMatchEtag: "hhh", 
        partitionKeyRangeId:"0"
    };
    var result = AzureCosmosClient->getDocumentList(databaseId, containerId, 10, options);
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    }
}

@test:Config{
    groups: ["document"], 
    dependsOn: ["test_createDocument"]
}
function test_GetOneDocument(){
    log:print("ACTION : GetOneDocument()");

    string databaseId = database.id;
    string containerId = container.id;

    var result = AzureCosmosClient->getDocument(databaseId, containerId, document.id, [1234]);
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    }  
}

@test:Config{
    groups: ["document"], 
    dependsOn: ["test_createDocument"]
}
function test_GetOneDocumentWithRequestOptions(){
    log:print("ACTION : GetOneDocumentWithRequestOptions()");

    string databaseId = database.id;
    string containerId = container.id;
    @tainted Document getDoc = {
        id: document.id, 
        partitionKey : [1234]  
    };
    DocumentGetOptions options = {
        consistancyLevel : "Eventual", 
        sessionToken: "tag", 
        ifNoneMatchEtag: "hhh"
    };

    var result = AzureCosmosClient->getDocument(databaseId, containerId, document.id, [1234], options);
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    }  
}

@test:Config{
    groups: ["document"], 
    dependsOn: [
        "test_createContainer", 
        "test_createDocument", 
        "test_GetOneDocument", 
        "test_GetOneDocumentWithRequestOptions", 
        "test_queryDocuments"
    ]
}
function test_deleteDocument(){
    log:print("ACTION : deleteDocument()");

    string databaseId = database.id;
    string containerId = container.id;

    var result = AzureCosmosClient->deleteDocument(databaseId, containerId, document.id, [1234]);  
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    }  
}

@test:Config{
    groups: ["document"], 
    dependsOn: ["test_createContainer"]
}
function test_queryDocuments(){
    log:print("ACTION : queryDocuments()");

    string databaseId = database.id;
    string containerId = container.id;
    int[] partitionKey = [1234];
    Query sqlQuery = {
        query: string `SELECT * FROM ${container.id.toString()} f WHERE f.Address.City = 'Seattle'`, 
        parameters: []
    };
    var result = AzureCosmosClient->queryDocuments(databaseId, containerId, sqlQuery, 10, [1234]);   
    if (result is stream<Document>){
        var document = result.next();
    } else {
        test:assertFail(msg = result.message());
    }   
}

@test:Config{
    groups: ["document"], 
    dependsOn: ["test_createContainer"]
}
function test_queryDocumentsWithRequestOptions(){
    log:print("ACTION : queryDocumentsWithRequestOptions()");

    string databaseId = database.id;
    string containerId = container.id;
    int[] partitionKey = [1234];
    Query sqlQuery = {
        query: string `SELECT * FROM ${container.id.toString()} f WHERE f.Address.City = 'Seattle'`, 
        parameters: []
    };
    ResourceQueryOptions options = {
        //sessionToken: "tag", 
        enableCrossPartition: true
    };

    var result = AzureCosmosClient->queryDocuments(databaseId, containerId, sqlQuery, 10, (), options);   
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
        var document = result.next();
    }   
}

@test:Config{
    groups: ["storedProcedure"], 
    dependsOn: ["test_createContainer"]
}
function test_createStoredProcedure(){
    log:print("ACTION : createStoredProcedure()");

    var uuid = createRandomUUIDBallerina();
    string databaseId = database.id;
    string containerId = container.id;
    string createSprocBody = "function (){\r\n    var context = getContext();\r\n    var response = context.getResponse();\r\n\r\n    response.setBody(\"Hello,  World\");\r\n}"; 
    StoredProcedure sp = {
        id: string `sproc_${uuid.toString()}`, 
        body:createSprocBody
    };

    var result = AzureCosmosClient->createStoredProcedure(databaseId, containerId, sp);  
    if (result is StoredProcedure){
        storedPrcedure = <@untainted> result;
    } else {
        test:assertFail(msg = result.message());
    }   
}

@test:Config{
    groups: ["storedProcedure"], 
    dependsOn: ["test_createStoredProcedure"]
}
function test_replaceStoredProcedure(){
    log:print("ACTION : replaceStoredProcedure()");

    string databaseId = database.id;
    string containerId = container.id;

    string replaceSprocBody = "function heloo(personToGreet){\r\n    var context = getContext();\r\n    var response = context.getResponse();\r\n\r\n    response.setBody(\"Hello,  \" + personToGreet);\r\n}";
    StoredProcedure sp = {
        id: storedPrcedure.id, 
        body: replaceSprocBody
    }; 
    var result = AzureCosmosClient->replaceStoredProcedure(databaseId, containerId, sp);  
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    }   
}

@test:Config{
    groups: ["storedProcedure"], 
    dependsOn: ["test_createContainer"]
}
function test_getAllStoredProcedures(){
    log:print("ACTION : getAllStoredProcedures()");

    string databaseId = database.id;
    string containerId = container.id;

    var result = AzureCosmosClient->listStoredProcedures(databaseId, containerId);   
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
        var storedProcedure = result.next();
    }  
}

@test:Config{
    groups: ["storedProcedure"], 
    dependsOn: ["test_replaceStoredProcedure"]
}
function test_executeOneStoredProcedure(){
    log:print("ACTION : executeOneStoredProcedure()");

    string databaseId = database.id;
    string containerId = container.id;
    string executeSprocId = storedPrcedure.id;
    string[] arrayofparameters = ["Sachi"];

    var result = AzureCosmosClient->executeStoredProcedure(databaseId, containerId, executeSprocId, arrayofparameters);   
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    }        
}

@test:Config{
    groups: ["storedProcedure"], 
    dependsOn: ["test_createStoredProcedure", "test_executeOneStoredProcedure", "test_getAllStoredProcedures"]
}
function test_deleteOneStoredProcedure(){
    log:print("ACTION : deleteOneStoredProcedure()");

    string databaseId = database.id;
    string containerId = container.id;
    string deleteSprocId = storedPrcedure.id;
    
    var result = AzureCosmosClient->deleteStoredProcedure(databaseId, containerId, deleteSprocId);   
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    }   
}

@test:Config{
    groups: ["userDefinedFunction"], 
    dependsOn: ["test_createContainer"]
}
function test_createUDF(){
    log:print("ACTION : createUDF()");

    var uuid = createRandomUUIDBallerina();
    string databaseId = database.id;
    string containerId = container.id;
    string udfId = string `udf_${uuid.toString()}`;
    string createUDFBody = "function tax(income){\r\n    if (income == undefined) \r\n        throw 'no input';\r\n    if ((income < 1000) \r\n        return income * 0.1;\r\n    else if ((income < 10000) \r\n        return income * 0.2;\r\n    else\r\n        return income * 0.4;\r\n}"; 
    UserDefinedFunction createUdf = {
        id: udfId, 
        body: createUDFBody
    };

    var result = AzureCosmosClient->createUserDefinedFunction(databaseId, containerId, createUdf);  
    if (result is UserDefinedFunction){
        udf = <@untainted> result;
    } else {
        test:assertFail(msg = result.message());
    }   
}

@test:Config{
    groups: ["userDefinedFunction"], 
    dependsOn: ["test_createContainer", "test_createUDF"]
}
function test_replaceUDF(){
    log:print("ACTION : replaceUDF()");

    string databaseId = database.id;
    string containerId = container.id;
    string replaceUDFBody = "function taxIncome(income){\r\n if (income == undefined) \r\n throw 'no input';\r\n if ((income < 1000) \r\n return income * 0.1;\r\n else if ((income < 10000) \r\n return income * 0.2;\r\n else\r\n return income * 0.4;\r\n}"; 
    UserDefinedFunction replacementUdf = {
        id: udf.id, 
        body:replaceUDFBody
    };

    var result = AzureCosmosClient->replaceUserDefinedFunction(databaseId, containerId, replacementUdf);  
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    }   
}

@test:Config{
    groups: ["userDefinedFunction"], 
    dependsOn: ["test_createContainer",  "test_createUDF"]
}
function test_listAllUDF(){
    log:print("ACTION : listAllUDF()");

    string databaseId = database.id;
    string containerId = container.id;

    var result = AzureCosmosClient->listUserDefinedFunctions(databaseId, containerId);  
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
        var userDefinedFunction = result.next();
    }  
}

@test:Config{
    groups: ["userDefinedFunction"], 
    dependsOn: ["test_replaceUDF", "test_listAllUDF"]
}
function test_deleteUDF(){
    log:print("ACTION : deleteUDF()");

    string deleteUDFId = udf.id;
    string databaseId = database.id;
    string containerId = container.id;

    var result = AzureCosmosClient->deleteUserDefinedFunction(databaseId, containerId, deleteUDFId);  
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    }
}

@test:Config{
    groups: ["trigger"], 
    dependsOn: ["test_createContainer"]
}
function test_createTrigger(){
    log:print("ACTION : createTrigger()");

    var uuid = createRandomUUIDBallerina();
    string databaseId = database.id;
    string containerId = container.id;
    string triggerId = string `trigger_${uuid.toString()}`;
    string createTriggerBody = "function tax(income){\r\n    if (income == undefined) \r\n        throw 'no input';\r\n    if ((income < 1000) \r\n        return income * 0.1;\r\n    else if ((income < 10000) \r\n        return income * 0.2;\r\n    else\r\n        return income * 0.4;\r\n}";
    string createTriggerOperation = "All"; 
    string createTriggerType = "Post"; 
    Trigger createTrigger = {
        id:triggerId, 
        body:createTriggerBody, 
        triggerOperation:createTriggerOperation, 
        triggerType: createTriggerType
    };

    var result = AzureCosmosClient->createTrigger(databaseId, containerId, createTrigger);  
    if (result is Trigger){
        trigger = <@untainted>result;
    } else {
        test:assertFail(msg = result.message());
    }   
}

@test:Config{
    groups: ["trigger"], 
    dependsOn: ["test_createTrigger"]
}
function test_replaceTrigger(){
    log:print("ACTION : replaceTrigger()");

    string databaseId = database.id;
    string containerId = container.id;
    string replaceTriggerBody = "function updateMetadata(){\r\n var context = getContext();\r\n var collection = context.getCollection();\r\n var response = context.getResponse();\r\n var createdDocument = response.getBody();\r\n\r\n // query for metadata document\r\n var filterQuery = 'SELECT * FROM root r WHERE r.id = \"_metadata\"';\r\n var accept = collection.queryDocuments(collection.getSelfLink(),  filterQuery, \r\n updateMetadataCallback);\r\n if (!accept) throw \"Unable to update metadata,  abort\";\r\n\r\n function updateMetadataCallback(err,  documents,  responseOptions){\r\n if (err) throw new Error(\"Error\" + err.message);\r\n if (documents.length != 1) throw 'Unable to find metadata document';\r\n var metadataDocument = documents[0];\r\n\r\n // update metadata\r\n metadataDocument.createdDocuments += 1;\r\n metadataDocument.createdNames += \" \" + createdDocument.id;\r\n var accept = collection.replaceDocument(metadataDocument._self, \r\n metadataDocument,  function(err,  docReplaced){\r\n if (err) throw \"Unable to update metadata,  abort\";\r\n });\r\n if (!accept) throw \"Unable to update metadata,  abort\";\r\n return; \r\n }";
    string replaceTriggerOperation = "All"; 
    string replaceTriggerType = "Post";
    Trigger replaceTrigger = {
        id: trigger.id, 
        body:replaceTriggerBody, 
        triggerOperation:replaceTriggerOperation, 
        triggerType: replaceTriggerType
    };

    var result = AzureCosmosClient->replaceTrigger(databaseId, containerId, replaceTrigger);  
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    }   
}

@test:Config{
    groups: ["trigger"], 
    dependsOn: ["test_createTrigger"]
}
function test_listTriggers(){
    log:print("ACTION : listTriggers()");

    string databaseId = database.id;
    string containerId = container.id;

    var result = AzureCosmosClient->listTriggers(databaseId, containerId);  
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
        var doc = result.next();
    } 
}

@test:Config{
    groups: ["trigger"], 
    dependsOn: ["test_replaceTrigger", "test_listTriggers"]
}
function test_deleteTrigger(){
    log:print("ACTION : deleteTrigger()");

    string deleteTriggerId = trigger.id;
    string databaseId = database.id;
    string containerId = container.id;

    var result = AzureCosmosClient->deleteTrigger(databaseId, containerId, deleteTriggerId);  
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    } 
}

@test:Config{
    groups: ["user"], 
    dependsOn: ["test_createDatabase"]
}
function test_createUser(){
    log:print("ACTION : createUser()");

    var uuid = createRandomUUIDBallerina();
    string databaseId = database.id;
    string userId = string `user_${uuid.toString()}`;

    var result = AzureCosmosClient->createUser(databaseId, userId);  
    if (result is User){
        test_user = <@untainted>result;
    } else {
        test:assertFail(msg = result.message());
    }   
}

@test:Config{
    groups: ["user"], 
    dependsOn: ["test_createUser", "test_getUser"]
}
function test_replaceUserId(){
    log:print("ACTION : replaceUserId()");

    var uuid = createRandomUUIDBallerina();
    string newReplaceId = string `user_${uuid.toString()}`;
    string databaseId = database.id;
    string replaceUser = test_user.id;

    var result = AzureCosmosClient->replaceUserId(databaseId, replaceUser, newReplaceId);  
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        test_user = <@untainted>result;
    }  
}

@test:Config{
    groups: ["user"], 
    dependsOn: ["test_createUser"]
}
function test_getUser(){
    log:print("ACTION : getUser()");

    Client AzureCosmosClient = new(config);
    string databaseId = database.id;
    string getUserId = test_user.id;

    var result = AzureCosmosClient->getUser(databaseId, getUserId);  
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    }  
}

@test:Config{
    groups: ["user"], 
    dependsOn: ["test_createUser"]
}
function test_listUsers(){
    log:print("ACTION : listUsers()");

    string databaseId = database.id;

    var result = AzureCosmosClient->listUsers(databaseId);  
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
        var doc = result.next();
    } 
}

@test:Config{
    groups: ["user"], 
    dependsOn: [
        "test_replaceUserId", 
        "test_deletePermission", 
        "test_createPermissionWithTTL", 
        "test_getCollection_Resource_Token"
    ]
}
function test_deleteUser(){
    log:print("ACTION : deleteUser()");

    string deleteUserId = test_user.id;
    string databaseId = database.id;

    var result = AzureCosmosClient->deleteUser(databaseId, deleteUserId);  
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    } 
}

@test:Config{
    groups: ["permission"], 
    dependsOn: ["test_createDatabase", "test_createUser"]
}
function test_createPermission(){
    log:print("ACTION : createPermission()");

    var uuid = createRandomUUIDBallerina();
    string databaseId = database.id;
    string permissionUserId = test_user.id;
    string permissionId = string `permission_${uuid.toString()}`;
    string permissionMode = "All";
    string permissionResource = string `dbs/${database?.resourceId.toString()}/colls/${container?.resourceId.toString()}`;
    Permission createPermission = {
        id: permissionId, 
        permissionMode: permissionMode, 
        resourcePath: permissionResource
    };

    var result = AzureCosmosClient->createPermission(databaseId, permissionUserId, createPermission);  
    if (result is Permission){
        permission = <@untainted>result;
    } else {
        test:assertFail(msg = result.message());
    }   
}

@test:Config{
    groups: ["permission"], 
    dependsOn: ["test_createDatabase", "test_createUser"]
}
function test_createPermissionWithTTL(){
    log:print("ACTION : createPermission()");

    var uuid = createRandomUUIDBallerina();
    string databaseId = database.id;
    string permissionUserId = test_user.id;
    string permissionId = string `permission_${uuid.toString()}`;
    string permissionMode = "All";
    string permissionResource = string `dbs/${database?.resourceId.toString()}/colls/${container?.resourceId.toString()}`;
    int validityPeriod = 9000;
    Permission createPermission = {
        id: permissionId, 
        permissionMode: permissionMode, 
        resourcePath: permissionResource
    };

    var result = AzureCosmosClient->createPermission(databaseId, permissionUserId, createPermission, validityPeriod);  
    if (result is Permission){
        permission = <@untainted>result;
    } else {
        test:assertFail(msg = result.message());
    }   
}

@test:Config{
    groups: ["permission"], 
    dependsOn: ["test_createPermission"]
}
function test_replacePermission(){
    log:print("ACTION : replacePermission()");

    string databaseId = database.id;
    string permissionUserId = test_user.id;
    string permissionId = permission.id;
    string permissionMode = "All";
    string permissionResource = string `dbs/${database.id}/colls/${container.id}`;
    Permission replacePermission = {
        id: permissionId, 
        permissionMode: permissionMode, 
        resourcePath: permissionResource
    };

    var result = AzureCosmosClient->replacePermission(databaseId, permissionUserId, replacePermission);  
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    }  
}

@test:Config{
    groups: ["permission"], 
    dependsOn: ["test_createPermission"]
}
function test_listPermissions(){
    log:print("ACTION : listPermissions()");

    string databaseId = database.id;
    string permissionUserId = test_user.id;

    var result = AzureCosmosClient->listPermissions(databaseId, permissionUserId);  
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
        var permission = result.next();
    } 
}

@test:Config{
    groups: ["permission"], 
    dependsOn: ["test_createPermission"]
}
function test_getPermission(){
    log:print("ACTION : getPermission()");

    string databaseId = database.id;
    string permissionUserId = test_user.id;
    string permissionId = permission.id;

    var result = AzureCosmosClient->getPermission(databaseId, permissionUserId, permissionId);  
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    }
}

@test:Config{
    groups: ["permission"], 
    dependsOn: [ "test_getPermission", "test_listPermissions", "test_replacePermission"]
}
function test_deletePermission(){
    log:print("ACTION : deletePermission()");

    string databaseId = database.id;
    string permissionUserId = test_user.id;
    string permissionId = permission.id;

    var result = AzureCosmosClient->deletePermission(databaseId, permissionUserId, permissionId);  
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        var output = "";
    } 
}

@test:Config{
    groups: ["offer"]
}
function test_listOffers(){
    log:print("ACTION : listOffers()");

    runtime:sleep(3000);
    var result = AzureCosmosClient->listOffers(10);  
    if (result is stream<Offer>){
        var offer = result.next();
    } else {
        test:assertFail(msg = result.message());
    }   
}

@test:Config{
    groups: ["offer"], 
    dependsOn: ["test_listOffers"]
}
function test_getOffer(){
    log:print("ACTION : getOffer()");

    //These fuctions can be depending on the list Offers
    var result = AzureCosmosClient->listOffers(10);  
    if (result is stream<Offer>){
        var doc = result.next();
        string offerId = doc["value"]["id"] is string? <string>doc["value"]["id"] : "";
        var result2 = AzureCosmosClient->getOffer(offerId);          
        if (result2 is error){
            test:assertFail(msg = result2.message());
        } else {
            var output = "";
        }  
    }  

}

@test:Config{
    groups: ["offer"],
    enable: false
}
function test_replaceOffer(){
    log:print("ACTION : replaceOffer()");

    //these fuctions can be depending on the list Offers
    var result = AzureCosmosClient->listOffers();  
    if (result is stream<Offer>){
        var doc = result.next();
        string offerId = doc["value"]["id"] is string? <string>doc["value"]["id"] : "";
        string resourceId = doc["value"]["resourceId"] is string? <string>doc["value"]["resourceId"] : "";

        Offer replaceOfferBody = {
            offerVersion: "V2", 
            offerType: "Invalid",    
            content: {  
                "offerThroughput": 600
            },  
            resourceSelfLink: string `dbs/${database?.resourceId.toString()}/colls/${container?.resourceId.toString()}/`,  
            resourceResourceId: string `${container?.resourceId.toString()}`, 
            id: offerId, 
            resourceId: resourceId
        };
        var result2 = AzureCosmosClient->replaceOffer(<@untainted>replaceOfferBody);  
        if (result2 is error){
            test:assertFail(msg = result2.message());
        } else {
            var output = "";
        }  
    }  
}

@test:Config{
    groups: ["offer"],
    enable: false
}
function test_replaceOfferWithOptionalParameter(){
    log:print("ACTION : replaceOfferWithOptionalParameter()");

    var result = AzureCosmosClient->listOffers();  
    if (result is stream<Offer>){
        var doc = result.next();
        string offerId = doc["value"]["id"] is string? <string>doc["value"]["id"] : "";
        string resourceId = doc["value"]["resourceId"] is string? <string>doc["value"]["resourceId"] : "";
        Offer replaceOfferBody = {
            offerVersion: "V2", 
            content: {  
                "offerThroughput": 600
            },  
            resourceSelfLink: string `dbs/${database?.resourceId.toString()}/colls/${container?.resourceId.toString()}/`,  
            resourceResourceId: string `${container?.resourceId.toString()}`, 
            id: offerId, 
            resourceId: resourceId
        };
        var result2 = AzureCosmosClient->replaceOffer(<@untainted>replaceOfferBody);  
        if (result2 is error){
            test:assertFail(msg = result2.message());
        } else {
            var output = "";
        }  
    }  
}

@test:Config{
    groups: ["offer"], 
    dependsOn: ["test_createDatabase",  "test_createContainer"]
}
function test_queryOffer(){
    log:print("ACTION : queryOffer()");

    runtime:sleep(3000);
    Query offerQuery = {
    query: string `SELECT * FROM ${container.id} f WHERE (f["_self"]) = "${container?.selfReference.toString()}"`
    };
    var result = AzureCosmosClient->queryOffer(offerQuery, 20);   
    if (result is stream<Offer>){
        var offer = result.next();
    } else {
        test:assertFail(msg = result.message());
    }     
}

@test:Config{
    groups: ["permission"], 
    dependsOn: ["test_createPermission"]
}
function test_getCollection_Resource_Token(){
    log:print("ACTION : createCollection_Resource_Token()");

    string databaseId = database.id;
    string permissionUserId = test_user.id;
    string permissionId = permission.id;

    var result = AzureCosmosClient->getPermission(databaseId, permissionUserId, permissionId);  
    if (result is error){
        test:assertFail(msg = result.message());
    } else {
        if (result?.token is string){
            AzureCosmosConfiguration configdb = {
                baseUrl : getConfigValue("BASE_URL"), 
                keyOrResourceToken : result?.token.toString(), 
                tokenType : "resource", 
                tokenVersion : getConfigValue("TOKEN_VERSION")
            };

            Client AzureCosmosClientDatabase = new(configdb);

            string containerId = container.id;

            var resultdb = AzureCosmosClientDatabase->getContainer(databaseId, containerId);
            if (resultdb is error){
                test:assertFail(msg = resultdb.message());
            } else {
                var output = "";
            }
        }
    }
}

isolated function getConfigValue(string key) returns string {
    return (system:getEnv(key) != "") ? system:getEnv(key) : config:getAsString(key);
}
