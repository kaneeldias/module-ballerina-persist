// Copyright (c) 2023 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
public client class InMemoryClient {

    private string[] keyFields;
    private function (string[]) returns stream<record {}, Error?> query;
    private function (anydata) returns record{}|InvalidKeyError queryOne;
    private map<function (record {}, string[]) returns record{}[]> associationsMethods;

    # Initializes the `SQLClient`.
    #
    # + dbClient - The `sql:Client`, which is used to execute SQL queries
    # + metadata - Metadata of the entity
    # + return - A `persist:Error` if the client creation fails
    public function init(TableMetadata metadata) returns Error? {
        self.keyFields = metadata.keyFields;
        self.query = metadata.query;
        self.queryOne = metadata.queryOne;
        self.associationsMethods = metadata.associationsMethods;
    }

    # Performs an SQL `SELECT` operation to read multiple entity records from the database.
    #
    # + rowType - The type description of the entity to be retrieved
    # + fields - The fields to be retrieved
    # + include - The associations to be retrieved
    # + return - A stream of records in the `rowType` type or a `persist:Error` if the operation fails
    public isolated function runReadQuery(typedesc<record {}> rowType, string[] fields = [], string[] include = [])
    returns stream<record {}, Error?> {
        return self.query(self.addKeyFields(fields));
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
        
        'object = filterRecord('object, self.addKeyFields(fields));
        check self.getManyRelations('object, fields, include, typeDescriptions);
        self.removeUnwantedFields('object, fields);

        do {
            return check 'object.cloneWithType(rowType);
        } on fail error e {
            return <Error>e;
        }
    }

    public isolated function getManyRelations(record {} 'object, string[] fields, string[] include, typedesc<record {}>[] typeDescriptions) returns Error? {
        foreach int i in 0..< include.length() {
            string entity = include[i];
            string[] relationFields = from string 'field in fields
                                      where 'field.startsWith(entity + "[].")
                                      select 'field.substring(entity.length() + 3, 'field.length());

            if relationFields.length() is 0 {
                continue;
            }

            function (record {}, string[]) returns record{}[] associationsMethod = self.associationsMethods.get(entity);
            record {}[] relations = associationsMethod('object, relationFields);
            'object[entity] = relations;
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

    public isolated function getKeyFields() returns string[] {
        return self.keyFields;
    }

    public isolated function addKeyFields(string[] fields) returns string[] {
        string[] updatedFields = fields.clone();

        foreach string key in self.keyFields {
            if updatedFields.indexOf(key) is () {
                updatedFields.push(key);
            }
        }
        return updatedFields;
    }

    private isolated function removeUnwantedFields(record{} 'object, string[] fields) {
        foreach string keyField in self.keyFields {
            if fields.indexOf(keyField) is () {
                _ = 'object.remove(keyField);
            }
        }
    }


}

# Represents the abstract persist client. This abstract object is used in the generated client.
public type AbstractPersistClient distinct object {
};
