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

import ballerina/sql;
import ballerina/jballerina.java;

isolated function stringToParameterizedQuery(string queryStr) returns sql:ParameterizedQuery {
    sql:ParameterizedQuery query = ``;
    query.strings = [queryStr];
    return query;
}

isolated function getKeyFromDuplicateKeyErrorMessage(string errorMessage) returns string|Error {
    int? startIndex = errorMessage.indexOf(".Duplicate entry '");
    int? endIndex = errorMessage.indexOf("' for key");
    
    if startIndex is () || endIndex is () {
        return <Error>error("Unable to determine key from DuplicateKey error message.");
    }

    string key = errorMessage.substring(startIndex+18, endIndex);
    return key;
}

isolated function convertToArray(typedesc<record {}> elementType, record {}[] arr) returns elementType[] = @java:Method {
    'class: "io.ballerina.stdlib.persist.Utils"
} external;

isolated function arrayToParameterizedQuery(string[] arr, sql:ParameterizedQuery delimiter = `,`) returns sql:ParameterizedQuery {
    sql:ParameterizedQuery query = stringToParameterizedQuery(arr[0]);
    foreach int i in 1..<arr.length() {
        query = sql:queryConcat(query, delimiter, stringToParameterizedQuery(arr[i]));
    }
    return query;
}

# Closes the entity stream.
#
# + customStream - Stream that needs to be closed
# + return - `()` if the operation is performed successfully or a `persist:Error` if the operation fails
public isolated function closeEntityStream(stream<anydata, sql:Error?>? customStream) returns Error? {
    if customStream is stream<anydata, sql:Error?> {
        sql:Error? e = customStream.close();
        if e is sql:Error {
            return <Error>error(e.message());
        }
    }
}

isolated function filterRecord(record {} 'object, string[] fields) returns record {} {
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
