import ballerina/io;
// Copyright (c) 2022 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

# The client used by the generated persist clients to abstract and 
# execute SQL queries that are required to perform CRUD operations.
public client class TableClient {

    private string entityName;
    private string[] keyFields;
    private table<record {}> key() 'table;
    private function () returns stream<record {}, Error?> query;
    private function (anydata) returns record{}|InvalidKeyError queryOne;

    # Initializes the `SQLClient`.
    #
    # + dbClient - The `sql:Client`, which is used to execute SQL queries
    # + metadata - Metadata of the entity
    # + return - A `persist:Error` if the client creation fails
    public function init(TableMetadata metadata, table<record {}> 'table, function () returns stream<record {}, Error?> query, function (anydata) returns record{}|InvalidKeyError queryOne) returns Error? {
        self.entityName = metadata.entityName;
        self.keyFields = metadata.keyFields;
        self.'table = 'table;
        self.query = query;
        self.queryOne = queryOne;
    }

    # Performs an SQL `SELECT` operation to read multiple entity records from the database.
    #
    # + rowType - The type description of the entity to be retrieved
    # + fields - The fields to be retrieved
    # + include - The associations to be retrieved
    # + return - A stream of records in the `rowType` type or a `persist:Error` if the operation fails
    public isolated function runReadQuery(typedesc<record {}> rowType, string[] fields = [], string[] include = [])
    returns stream<record {}, Error?> {
        return from record{} 'object in self.query()
               select self.filterRecord('object, fields);
    }

    # Performs an SQL `SELECT` operation to read a single entity record from the database.
    #
    # + rowType - The type description of the entity to be retrieved
    # + rowTypeWithIdFields - The type description of the entity to be retrieved with the key fields included
    # + key - The value of the key (to be used as the `WHERE` clauses)
    # + fields - The fields to be retrieved
    # + include - The relations to be retrieved (SQL `JOINs` to be performed)
    # + typeDescriptions - The type descriptions of the relations to be retrieved
    # + return - A record in the `rowType` type or a `persist:Error` if the operation fails
    public isolated function runReadByKeyQuery(typedesc<record {}> rowType, typedesc<record {}> rowTypeWithIdFields, anydata key, string[] fields = [], string[] include = [], typedesc<record {}>[] typeDescriptions = []) returns record {}|Error {        
        record {} 'object = check self.queryOne(key);
        'object = self.filterRecord('object, fields);
        do {
            return check 'object.cloneWithType(rowType);
        } on fail error e {
            return <Error>e;
        }
    }

    public isolated function getKey(anydata|record {} 'object) returns anydata|record {} {
        record {} keyRecord = {};

        if self.keyFields.length() == 1 && 'object is record {} {
            return 'object[self.keyFields[0]];
        }
        
        if 'object is record {} {
            foreach string key in self.keyFields {
                keyRecord[key] = 'object[key];
            }
        } else {
            keyRecord[self.keyFields[0]] = 'object;
        }
        return keyRecord;
    }

    private isolated function filterRecord(record {} 'object, string[] fields) returns record {} {
        io:println('object);
        record {} retrieved = {};
        foreach string 'field in fields {

            // ignore many relations
            if 'field.includes("[]") {
                continue; 
            }

            // if field is part of a relation
            if 'field.includes(".") {

                int splitIndex = <int>'field.indexOf(".");
                string relation = 'field.substring(0, splitIndex);
                string innerField = 'field.substring(splitIndex + 1, 'field.length());

                if 'object[relation] is record {} {
                    anydata val = (<record {}>'object[relation])[innerField];

                    if !(retrieved[relation] is record {}) {
                        retrieved[relation] = {};
                    }

                    record {} innerRecord = <record {}>'retrieved[relation];
                    innerRecord[innerField] = val;
                }
            } else {
                retrieved['field] = 'object['field];
            }

        }
        return retrieved;
    }


}

# Represents the abstract persist client. This abstract object is used in the generated client.
public type AbstractPersistClient distinct object {
};
