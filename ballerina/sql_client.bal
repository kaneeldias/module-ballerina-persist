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

import ballerina/io;
import ballerina/sql;

# The client used by the generated persist clients to abstract and 
# execute SQL queries that are required to perform CRUD operations.
public client class SQLClient {

    private final sql:Client dbClient;

    private string entityName;
    private sql:ParameterizedQuery tableName;
    private map<FieldMetadata> fieldMetadata;
    private string[] keyFields;
    private map<JoinMetadata> joinMetadata = {};

    # Initializes the `SQLClient`.
    #
    # + dbClient - The `sql:Client`, which is used to execute SQL queries
    # + metadata - Metadata of the entity
    # + return - A `persist:Error` if the client creation fails
    public function init(sql:Client dbClient, Metadata metadata) returns Error? {
        self.entityName = metadata.entityName;
        self.tableName = metadata.tableName;
        self.fieldMetadata = metadata.fieldMetadata;
        self.keyFields = metadata.keyFields;
        self.dbClient = dbClient;
        if metadata.joinMetadata is map<JoinMetadata> {
            self.joinMetadata = <map<JoinMetadata>>metadata.joinMetadata;
        }
    }

    # Performs a batch SQL `INSERT` operation to insert entity instances into a table.
    #
    # + insertRecords - The entity records to be inserted into the table
    # + return - An `sql:ExecutionResult[]` containing the metadata of the query execution
    #            or a `persist:Error` if the operation fails
    public isolated function runBatchInsertQuery(record {}[] insertRecords) returns sql:ExecutionResult[]|Error {
        sql:ParameterizedQuery[] insertQueries = 
            from record {} insertRecord in insertRecords
            select sql:queryConcat(`INSERT INTO `, self.tableName, ` (`, self.getInsertColumnNames(), ` ) `, `VALUES `, self.getInsertQueryParams(insertRecord));
        
        sql:ExecutionResult[]|sql:Error result = self.dbClient->batchExecute(insertQueries);

        if result is sql:Error {
            if result.message().indexOf("Duplicate entry ") != () {
                string duplicateKey = check getKeyFromDuplicateKeyErrorMessage(result.message());
                return <DuplicateKeyError>error(string `A ${self.entityName} entity with the key '${duplicateKey}' already exists.`);
            }

            return <Error>error(result.message());
        }

        return result;
    }

    # Performs an SQL `SELECT` operation to read a single entity record from the database.
    #
    # + rowType - The type description of the entity to be retrieved
    # + key - The value of the key (to be used as the `WHERE` clauses)
    # + fields - The fields to be retrieved
    # + include - The relations to be retrieved (SQL `JOINs` to be performed)
    # + typeDescriptions - The type descriptions of the relations to be retrieved
    # + return - A record in the `rowType` type or a `persist:Error` if the operation fails
    public isolated function runReadByKeyQuery(typedesc<record {}> rowType, anydata key, string[] fields = [], string[] include = [], typedesc<record {}>[] typeDescriptions = []) returns record {}|Error {
        sql:ParameterizedQuery query = sql:queryConcat(
            `SELECT `, self.getSelectColumnNames(fields, include), ` FROM `, self.tableName, ` AS `, stringToParameterizedQuery(self.entityName)
        );

        boolean groupRequired = false;

        foreach string joinKey in self.joinMetadata.keys() {
            if include.indexOf(joinKey) != () {
                JoinMetadata joinMetadata = self.joinMetadata.get(joinKey);
                if joinMetadata.'type == MANY_TO_ONE {
                    groupRequired = true;
                }
                query = sql:queryConcat(query, ` LEFT JOIN `, stringToParameterizedQuery(joinMetadata.refTable + " " + joinKey),
                                        ` ON `, check self.getJoinFilters(joinKey, joinMetadata.refColumns, <string[]>joinMetadata.joinColumns));
            }
        }

        query = sql:queryConcat(query, ` WHERE `, check self.getGetKeyWhereClauses(key));

        if groupRequired {
            query = sql:queryConcat(query, ` GROUP BY `, stringToParameterizedQuery(joinArray(self.keyFields)));
        }

        io:println(query);

        record {}|sql:Error result = self.dbClient->queryRow(query, rowType);

        if result is sql:NoRowsError {
            return <InvalidKeyError>error(
                string `A record does not exist for '${self.entityName}' for key ${key.toBalString()}.`);
        }

        if result is record {} {
            //check self.getManyRelations(result, fields, include, typeDescriptions);
        }

        if result is sql:Error {
            return <Error>error(result.message());
        }
        return result;
    }

    # Performs an SQL `SELECT` operation to read multiple entity records from the database.
    #
    # + rowType - The type description of the entity to be retrieved
    # + fields - The fields to be retrieved
    # + include - The associations to be retrieved
    # + return - A stream of records in the `rowType` type or a `persist:Error` if the operation fails
    public isolated function runReadQuery(typedesc<record {}> rowType, string[] fields = [], string[] include = [])
    returns stream<record {}, sql:Error?>|Error {
        sql:ParameterizedQuery query = sql:queryConcat(
            `SELECT `, self.getSelectColumnNames(fields, include), ` FROM `, self.tableName, ` `, stringToParameterizedQuery(self.entityName)
        );

        boolean groupRequired = false;

        string[] joinKeys = self.joinMetadata.keys();
        foreach string joinKey in joinKeys {
            if include.indexOf(joinKey) != () {
                JoinMetadata joinMetadata = self.joinMetadata.get(joinKey);
                if joinMetadata.'type == MANY_TO_ONE {
                    groupRequired = true;
                }

                query = sql:queryConcat(query, ` LEFT JOIN `, stringToParameterizedQuery(joinMetadata.refTable + " " + joinKey),
                                        ` ON `, check self.getJoinFilters(joinKey, joinMetadata.refColumns, <string[]>joinMetadata.joinColumns));
            }
        }

        if groupRequired {
            query = sql:queryConcat(query, ` GROUP BY `, stringToParameterizedQuery(joinArray(self.keyFields)));
        }

        io:println(query);

        stream<record {}, sql:Error?> resultStream = self.dbClient->query(query, rowType);
        return resultStream;
    }

    # Performs an SQL `UPDATE` operation to update multiple entity records in the database.
    #
    # + key - the key of the entity
    # + updateRecord - the record to be updated
    # + updateAssociations - The associations that should be updated
    # + return - `()` if the operation is performed successfully.
    # A `ForeignKeyConstraintViolationError` if the operation violates a foreign key constraint.
    # A `persist:Error` if the operation fails due to another reason.
    public isolated function runUpdateQuery(anydata key, record {} updateRecord, string[] updateAssociations = []) returns ForeignKeyConstraintViolationError|Error? {
        sql:ParameterizedQuery query = sql:queryConcat(`UPDATE `, self.tableName, stringToParameterizedQuery(" " + self.entityName), ` SET`, check self.getSetClauses(updateRecord, updateAssociations));
        query = sql:queryConcat(query, ` WHERE`, check self.getWhereClauses(self.getKey(key)));

        sql:ExecutionResult|sql:Error? e = self.dbClient->execute(query);
        if e is sql:Error {
            if e.message().indexOf("a foreign key constraint fails ") is int {
                return <ForeignKeyConstraintViolationError>error(e.message());
            }
            else {
                return <Error>error(e.message());
            }
        }
    }

    # Performs an SQL `DELETE` operation to delete an entity record from the database.
    #
    # + deleteKey - The key used to delete an entity record
    # + return - `()` if the operation is performed successfully or a `persist:Error` if the operation fails
    public isolated function runDeleteQuery(anydata deleteKey) returns Error? {
        sql:ParameterizedQuery query = sql:queryConcat(`DELETE FROM `, self.tableName, stringToParameterizedQuery(" " + self.entityName));
        query = sql:queryConcat(query, ` WHERE`, check self.getWhereClauses(self.getKey(deleteKey)));
        sql:ExecutionResult|sql:Error e = self.dbClient->execute(query);

        if e is sql:Error {
            return <Error>error(e.message());
        }
    }

    # Retrieves the values of the 'many' side of an association.
    #
    # + 'object - The record to which the retrieved records should be appended
    # + fields - The fields to be retrieved
    # + include - The relations to be retrieved (SQL `JOINs` to be performed)
    # + typeDescriptions - The type descriptions of the relations to be retrieved
    # + return - `()` if the operation is performed successfully or a `persist:Error` if the operation fails
    public isolated function getManyRelations(anydata 'object, string[] fields, string[] include, typedesc<record {}>[] typeDescriptions) returns Error? {
        if !('object is record {}) {
            return <Error>error("The 'object' parameter should be a record");
        }
        foreach string joinKey in self.joinMetadata.keys() {
            sql:ParameterizedQuery query = ``;
            JoinMetadata joinMetadata = self.joinMetadata.get(joinKey);

            if include.indexOf(joinKey) != () && joinMetadata.'type == MANY_TO_ONE {
                map<string> whereFilter = {};
                foreach int i in 0 ..< joinMetadata.refColumns.length() {
                    whereFilter[joinMetadata.refColumns[i]] = 'object[check self.getFieldFromColumn(joinMetadata.joinColumns[i])].toBalString();
                }

                query = sql:queryConcat(
                    ` SELECT `, stringToParameterizedQuery(joinArray(self.getManyRelationColumnNames(joinMetadata.fieldName, fields))),
                    ` FROM `, stringToParameterizedQuery(joinMetadata.refTable),
                    ` WHERE`, check self.getWhereClauses(whereFilter, true)
                );

                stream<record {}, sql:Error?> joinStream = self.dbClient->query(query, typeDescriptions[<int>include.indexOf(joinKey)]);
                record {}[]|error arr = from record {} item in joinStream
                    select item;

                if arr is error {
                    return <Error>error(arr.message());
                }
                
                'object[joinMetadata.fieldName] = convertToArray(typeDescriptions[<int>include.indexOf(joinKey)], arr);
            }
        }
    }

    private isolated function getKey(anydata|record {} 'object) returns record {} {
        record {} keyRecord = {};
        
        if 'object is record {} {
            foreach string key in self.keyFields {
                keyRecord[key] = 'object[key];
            }
        } else {
            keyRecord[self.keyFields[0]] = 'object;
        }
        return keyRecord;
    }

    private isolated function getInsertQueryParams(record {} 'object) returns sql:ParameterizedQuery {
        sql:ParameterizedQuery params = `(`;
        int columnCount = 0;
        foreach string key in self.fieldMetadata.keys() {
            FieldMetadata fieldMetadata = self.fieldMetadata.get(key);
            if !isInsertableField(fieldMetadata) {
                continue;
            }
            if columnCount > 0 {
                params = sql:queryConcat(params, `,`);
            }

            
            params = sql:queryConcat(params, `${<sql:Value>'object[key]}`);
            columnCount = columnCount + 1;
        }
        params = sql:queryConcat(params, `)`);
        return params;
    }

    private isolated function getInsertColumnNames() returns sql:ParameterizedQuery {
        sql:ParameterizedQuery params = ` `;
        string[] keys = self.fieldMetadata.keys();
        int columnCount = 0;
        foreach string key in keys {
            FieldMetadata fieldMetadata = self.fieldMetadata.get(key);
            if !isInsertableField(fieldMetadata) {
                continue;
            }
            if columnCount > 0 {
                params = sql:queryConcat(params, `, `);
            }
            params = sql:queryConcat(params, stringToParameterizedQuery((<SimpleFieldMetadata>fieldMetadata).columnName));
            columnCount = columnCount + 1;
        }
        return params;
    }

    private isolated function getSelectColumnNames(string[] fields, string[] include) returns sql:ParameterizedQuery {
        sql:ParameterizedQuery params = ` `;
        int columnCount = 0;

        foreach string key in self.fieldMetadata.keys() {
            if fields != [] && fields.indexOf(key) is () {
                continue;
            }
            string fieldName = self.getFieldFromKey(key);

            FieldMetadata fieldMetadata = self.fieldMetadata.get(key);
            if fieldMetadata is SimpleFieldMetadata {
                if columnCount > 0 {
                    params = sql:queryConcat(params, `, `);
                }
                params = sql:queryConcat(params, stringToParameterizedQuery(self.entityName + "." + fieldMetadata.columnName + " AS `" + key + "`"));
                columnCount = columnCount + 1;
            } else if fields.indexOf(key) != () {
                if !key.includes("[]") {
                    if columnCount > 0 {
                        params = sql:queryConcat(params, `, `);
                    }
                    params = sql:queryConcat(params, stringToParameterizedQuery(
                        fieldName + "." + fieldMetadata.relation.refField + 
                        " AS `" + fieldName + "." + fieldMetadata.relation.refField + "`"
                    ));
                    columnCount = columnCount + 1;
                }
            }
        }

        foreach string joinMetadataKey in self.joinMetadata.keys() {
            if include.indexOf(joinMetadataKey) != () && self.joinMetadata.get(joinMetadataKey).'type == MANY_TO_ONE {
                string[] columnNames = self.getManyRelationColumnNames(joinMetadataKey, fields);
                if columnCount > 0 {
                    params = sql:queryConcat(params, `, `);
                }

                string jsonQuery = "CONCAT('[', GROUP_CONCAT(JSON_OBJECT(";
                foreach int i in 0..<columnNames.length() {
                    if i > 0 {
                        jsonQuery = jsonQuery + ", ";
                    }
                    string columnName = columnNames[i];
                    jsonQuery = jsonQuery + "'" + columnName + "', " + joinMetadataKey + "." + columnName;
                }
                jsonQuery = jsonQuery + ") separator ',') , ']') AS " + joinMetadataKey;

                params = sql:queryConcat(params, stringToParameterizedQuery(jsonQuery));
                columnCount = columnCount + 1;
            }
        }

        return params;
    }

    private isolated function getManyRelationColumnNames(string prefix, string[] fields) returns string[] {
        string[] columnNames = [];
        foreach string key in fields {
            if key.indexOf(prefix + "[].") is () {
                continue;
            }

            FieldMetadata fieldMetadata = self.fieldMetadata.get(key);
            if fieldMetadata is SimpleFieldMetadata {
                continue;
            }

            columnNames.push(fieldMetadata.relation.refField);
        }
        return columnNames;
    }

    private isolated function getGetKeyWhereClauses(anydata key) returns sql:ParameterizedQuery|Error {
        map<anydata> filter = {};

        if key is map<any> {
            filter = key;
        } else {
            filter[self.keyFields[0]] = key;
        }

        return check self.getWhereClauses(filter);
    }

    private isolated function getWhereClauses(map<anydata> filter, boolean ignoreFieldCheck = false) returns sql:ParameterizedQuery|Error {
        sql:ParameterizedQuery query = ` `;

        string[] keys = filter.keys();
        foreach int i in 0 ..< keys.length() {
            if i > 0 {
                query = sql:queryConcat(query, ` AND `);
            }
            if ignoreFieldCheck {
                query = sql:queryConcat(query, stringToParameterizedQuery(keys[i] + " = " + filter[keys[i]].toString()));
            } else {
                query = sql:queryConcat(query, stringToParameterizedQuery(self.entityName + "."), self.getFieldParamQuery(keys[i]), ` = ${<sql:Value>filter[keys[i]]}`);
            }
        }
        return query;
    }

    private isolated function getSetClauses(record {} 'object, string[] updateAssociations = []) returns sql:ParameterizedQuery|Error {
        sql:ParameterizedQuery query = ` `;
        int count = 0;
        foreach string key in 'object.keys() {
            if !self.fieldMetadata.hasKey(key) {
                continue;
            }

            sql:ParameterizedQuery fieldName = self.getFieldParamQuery(key);
            if count > 0 {
                query = sql:queryConcat(query, `, `);
            }
            query = sql:queryConcat(query, fieldName, ` = ${<sql:Value>'object[key]}`);
            count = count + 1;
        }
        return query;
    }

    private isolated function getJoinFilters(string joinKey, string[] refFields, string[] joinColumns) returns sql:ParameterizedQuery|Error {
        sql:ParameterizedQuery query = ` `;
        foreach int i in 0 ..< refFields.length() {
            if i > 0 {
                query = sql:queryConcat(query, ` AND `);
            }
            sql:ParameterizedQuery filterQuery = stringToParameterizedQuery(joinKey + "." + refFields[i] + " = " + self.entityName + "." + joinColumns[i]);
            query = sql:queryConcat(query, filterQuery);
        }
        return query;
    }

    private isolated function getFieldParamQuery(string fieldName) returns sql:ParameterizedQuery {
        SimpleFieldMetadata fieldMetadata = <SimpleFieldMetadata>self.fieldMetadata.get(fieldName);
        return stringToParameterizedQuery(fieldMetadata.columnName);
    }

    private isolated function getFieldFromColumn(string columnName) returns string|FieldDoesNotExistError {
        foreach string key in self.fieldMetadata.keys() {
            FieldMetadata fieldMetadata = self.fieldMetadata.get(key);
            if fieldMetadata is EntityFieldMetadata {
                continue;
            }

            if fieldMetadata.columnName == columnName {
                return key;
            }
        }

        return <FieldDoesNotExistError>error(
            string `A field corresponding to column '${columnName}' does not exist in entity '${self.entityName}'.`);
    }

    private isolated function getFieldFromKey(string key) returns string {
        int? splitIndex = key.indexOf(".");
        if splitIndex is () {
            return key;
        }
        return key.substring(0, splitIndex);
    }

}

# Represents the abstract persist client. This abstract object is used in the generated client.
public type AbstractPersistClient distinct object {
};
