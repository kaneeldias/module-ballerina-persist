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
    private table<record {}> key() 'table;

    # Initializes the `SQLClient`.
    #
    # + dbClient - The `sql:Client`, which is used to execute SQL queries
    # + metadata - Metadata of the entity
    # + return - A `persist:Error` if the client creation fails
    public function init(sql:Client dbClient, Metadata metadata, table<record {}> 'table = table[]) returns Error? {
        self.entityName = metadata.entityName;
        self.tableName = metadata.tableName;
        self.fieldMetadata = metadata.fieldMetadata;
        self.keyFields = metadata.keyFields;
        self.dbClient = dbClient;
        if metadata.joinMetadata is map<JoinMetadata> {
            self.joinMetadata = <map<JoinMetadata>>metadata.joinMetadata;
        }
        self.'table = 'table;
    }

    # Performs a batch SQL `INSERT` operation to insert entity instances into a table.
    #
    # + insertRecords - The entity records to be inserted into the table
    public isolated function runBatchInsertQuery(record {}[] insertRecords) returns DuplicateKeyError? {
        foreach record {} insertRecord in insertRecords {
            from record{} 'object in self.'table
                where self.getKey('object) == self.getKey(insertRecord)
                do {
                    break;
                };
            
            self.'table.add(insertRecord);
        }
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
        record {}[] x = [];
        io:println(self.'table);
        from record{} 'object in self.'table
            where self.getKey('object) == key
            do {
                x.push('object);
            };

        if x.length() == 0 {
            io:println(string `A record does not exist for '${self.entityName}' for key ${key.toBalString()}.`);
            return <InvalidKeyError>error(string `A record does not exist for '${self.entityName}' for key ${key.toBalString()}.`);        
        }
        
        return x[0];
    }

    # Performs an SQL `SELECT` operation to read multiple entity records from the database.
    #
    # + rowType - The type description of the entity to be retrieved
    # + fields - The fields to be retrieved
    # + include - The associations to be retrieved
    # + return - A stream of records in the `rowType` type or a `persist:Error` if the operation fails
    public isolated function runReadQuery(typedesc<record {}> rowType, string[] fields = [], string[] include = [])
    returns stream<record {}, sql:Error?>|Error {
        stream<record{}, sql:Error?> resultStream = from record {} 'object in self.'table
                                                    select 'object;
        return resultStream;
    }

    # Performs an SQL `UPDATE` operation to update multiple entity records in the database.
    #
    # + key - the key of the entity
    # + updateRecord - the record to be updated
    # + return - `()` if the operation is performed successfully.
    # A `ForeignKeyConstraintViolationError` if the operation violates a foreign key constraint.
    # A `persist:Error` if the operation fails due to another reason.
    public isolated function runUpdateQuery(anydata key, record {} updateRecord) returns ForeignKeyConstraintViolationError|Error? {
        sql:ParameterizedQuery query = check self.getUpdateQuery(updateRecord);
        query = sql:queryConcat(query, check self.getWhereQuery(self.getKey(key)));

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
        sql:ParameterizedQuery query = self.getDeleteQuery();
        query = sql:queryConcat(query, check self.getWhereQuery(deleteKey));
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

        foreach string joinKey in self.getManyRelationFields(include) {
            sql:ParameterizedQuery query = ``;
            JoinMetadata joinMetadata = self.joinMetadata.get(joinKey);

            map<string> whereFilter = check self.getManyRelationWhereFilter('object, joinMetadata);
            typedesc<record {}> joinRelationTypedesc = self.getJoinRelationTypedescription(typeDescriptions, include, joinKey);

            query = sql:queryConcat(
                ` SELECT `, self.getManyRelationColumnNames(joinMetadata.fieldName, fields),
                ` FROM `, stringToParameterizedQuery(joinMetadata.refTable),
                ` WHERE`, check self.getWhereClauses(whereFilter, true)
            );

            stream<record {}, sql:Error?> joinStream = self.dbClient->query(query, joinRelationTypedesc);
            record {}[]|error arr = from record {} item in joinStream
                select item;

            if arr is error {
                return <Error>error(arr.message());
            }
            
            'object[joinMetadata.fieldName] = convertToArray(joinRelationTypedesc, arr);
        }
    }

    public isolated function getKeyFields() returns string[] {
        return self.keyFields;
    }

    private isolated function getKey(anydata|record {} 'object) returns anydata|record {} {
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

    private isolated function getInsertQueryParams(record {} 'object) returns sql:ParameterizedQuery {
        sql:ParameterizedQuery params = `(`;
        int columnCount = 0;

        foreach string key in self.getInsertableFields() {
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
        int columnCount = 0;
        
        foreach string key in self.getInsertableFields() {
            FieldMetadata fieldMetadata = self.fieldMetadata.get(key);
            if columnCount > 0 {
                params = sql:queryConcat(params, `, `);
            }

            params = sql:queryConcat(params, stringToParameterizedQuery((<SimpleFieldMetadata>fieldMetadata).columnName));
            columnCount = columnCount + 1;
        }
        return params;
    }

    private isolated function getSelectColumnNames(string[] fields) returns sql:ParameterizedQuery {
        string[] columnNames = [];

        foreach string key in self.getSelectableFields(fields) {
            string fieldName = self.getFieldFromKey(key);
            FieldMetadata fieldMetadata = self.fieldMetadata.get(key);

            if fieldMetadata is SimpleFieldMetadata {
                //  column is in the current entity's table
                columnNames.push(self.entityName + "." + fieldMetadata.columnName + " AS `" + key + "`");
            } else {
                // column is in another entity's table
                columnNames.push(fieldName + "." + fieldMetadata.relation.refField + " AS `" + fieldName + "." + fieldMetadata.relation.refField + "`");
            }

        }
        return arrayToParameterizedQuery(columnNames);
    }

    private isolated function getManyRelationColumnNames(string prefix, string[] fields) returns sql:ParameterizedQuery {
        string[] columnNames = [];
        foreach string key in fields {
            if key.indexOf(prefix + "[].") is () { // many relation columns will contain "[]" in their FieldMetadata key
                continue;
            }

            FieldMetadata fieldMetadata = self.fieldMetadata.get(key);
            if fieldMetadata is SimpleFieldMetadata {
                continue;
            }

            string columnName = fieldMetadata.relation.refField;
            columnNames.push(columnName);
        }
        return arrayToParameterizedQuery(columnNames);
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
                query = sql:queryConcat(query, stringToParameterizedQuery(self.entityName + "." + self.getColumnFromField(keys[i])), ` = ${<sql:Value>filter[keys[i]]}`);
            }
        }
        return query;
    }

    private isolated function getSetClauses(record {} 'object, string[] updateAssociations = []) returns sql:ParameterizedQuery|Error {
        sql:ParameterizedQuery query = ` `;
        int count = 0;
        foreach string key in 'object.keys() {
            sql:ParameterizedQuery columnName = stringToParameterizedQuery(self.getColumnFromField(key));
            if count > 0 {
                query = sql:queryConcat(query, `, `);
            }
            query = sql:queryConcat(query, columnName, ` = ${<sql:Value>'object[key]}`);
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

    private isolated function getColumnFromField(string fieldName) returns string {
        SimpleFieldMetadata fieldMetadata = <SimpleFieldMetadata>self.fieldMetadata.get(fieldName);
        return fieldMetadata.columnName;
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

        return <FieldDoesNotExistError>error(string `A field corresponding to column '${columnName}' does not exist in entity '${self.entityName}'.`);
    }

    private isolated function getFieldFromKey(string key) returns string {
        int? splitIndex = key.indexOf(".");
        if splitIndex is () {
            return key;
        }
        return key.substring(0, splitIndex);
    }

    private isolated function getInsertQueries(record {}[] insertRecords) returns sql:ParameterizedQuery[] {
        return from record {} insertRecord in insertRecords
               select sql:queryConcat(`INSERT INTO `, self.tableName, ` (`, self.getInsertColumnNames(), ` ) `, `VALUES `, self.getInsertQueryParams(insertRecord));
    }

    private isolated function getSelectQuery(string[] fields) returns sql:ParameterizedQuery {
        return sql:queryConcat(
            `SELECT `, self.getSelectColumnNames(fields), ` FROM `, self.tableName, ` AS `, stringToParameterizedQuery(self.entityName)
        );
    }

    private isolated function getWhereQuery(anydata key) returns sql:ParameterizedQuery|Error {
        return sql:queryConcat(` WHERE `, check self.getGetKeyWhereClauses(key));
    }

    private isolated function getUpdateQuery(record {} updateRecord) returns sql:ParameterizedQuery|Error {
        return sql:queryConcat(`UPDATE `, self.tableName, stringToParameterizedQuery(" " + self.entityName), ` SET `, check self.getSetClauses(updateRecord));
    }

    private isolated function getDeleteQuery() returns sql:ParameterizedQuery {
        return sql:queryConcat(`DELETE FROM `, self.tableName, stringToParameterizedQuery(" " + self.entityName));
    }
 
    private isolated function getJoinFields(string[] include) returns string[] {
        string[] joinFields = [];
        foreach string joinKey in self.joinMetadata.keys() {
            JoinMetadata joinMetadata = self.joinMetadata.get(joinKey);
            if include.indexOf(joinKey) != () && (joinMetadata.'type == ONE_TO_ONE  || joinMetadata.'type == ONE_TO_MANY) {
                joinFields.push(joinKey);
            }
        }
        return joinFields;
    }

    private isolated function getManyRelationFields(string[] include) returns string[] {
        return from string joinKey in self.joinMetadata.keys()
               let JoinMetadata joinMetadata = self.joinMetadata.get(joinKey)
               where include.indexOf(joinKey) != () && joinMetadata.'type == MANY_TO_ONE
               select joinKey;
    }

    private isolated function getManyRelationWhereFilter(record{} 'object, JoinMetadata joinMetadata) returns map<string>|Error {
        map<string> whereFilter = {};
        foreach int i in 0 ..< joinMetadata.refColumns.length() {
            whereFilter[joinMetadata.refColumns[i]] = 'object[check self.getFieldFromColumn(joinMetadata.joinColumns[i])].toBalString();
        }
        return whereFilter;
    }

    private isolated function getJoinQuery(string joinKey) returns sql:ParameterizedQuery|Error {
        JoinMetadata joinMetadata = self.joinMetadata.get(joinKey);
        return sql:queryConcat(` LEFT JOIN `, stringToParameterizedQuery(joinMetadata.refTable + " " + joinKey),
                               ` ON `, check self.getJoinFilters(joinKey, joinMetadata.refColumns, <string[]>joinMetadata.joinColumns));
    }

    private isolated function getJoinRelationTypedescription(typedesc<record {}>[] typedescriptions, string[] include, string joinKey) returns typedesc<record {}> {
        return typedescriptions[<int>include.indexOf(joinKey)];
    }

    private isolated function removeUnwantedFields(record{} 'object, string[] fields) {
        foreach string keyField in self.keyFields {
            if fields.indexOf(keyField) is () {
                _ = 'object.remove(keyField);
            }
        }
    }

    private isolated function getInsertableFields() returns string[] {
        return from string key in self.fieldMetadata.keys()
               where self.fieldMetadata.get(key) is SimpleFieldMetadata
               select key;
    }

    private isolated function getSelectableFields(string[] fields) returns string[] {
        return from string key in self.fieldMetadata.keys()
               where (fields.indexOf(key) != () || self.keyFields.indexOf(key) != ()) && !key.includes("[]")
               select key;
    }
}

# Represents the abstract persist client. This abstract object is used in the generated client.
public type AbstractPersistClient distinct object {
};
