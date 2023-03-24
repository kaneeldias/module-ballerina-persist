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
import ballerina/test;

Building building1 = {
    buildingCode: "building-1",
    city: "Colombo",
    state: "Western Province",
    country: "Sri Lanka",
    postalCode: "10370",
    'type: "rented"
};

Building invalidBuilding = {
    buildingCode: "building-2",
    city: "Colombo",
    state: "Western Province",
    country: "Sri Lanka",
    postalCode: "10370",
    'type: "owned"
};

BuildingInsert building2 = {
    buildingCode: "building-2",
    city: "Manhattan",
    state: "New York",
    country: "USA",
    postalCode: "10570",
    'type: "owned"
};

BuildingInsert building3 = {
    buildingCode: "building-3",
    city: "London",
    state: "London",
    country: "United Kingdom",
    postalCode: "39202",
    'type: "rented"
};

Building updatedBuilding1 = {
    buildingCode: "building-1",
    city: "Galle",
    state: "Southern Province",
    country: "Sri Lanka",
    postalCode: "10890",
    'type: "owned"
};

@test:Config {
    groups: ["building"]
}
function buildingCreateTest() returns error? {
    RainierClient rainierClient = check new ();
    
    string[] buildingCodes = check rainierClient->/buildings.post([building1]);    
    test:assertEquals(buildingCodes, [building1.buildingCode]);

    Building buildingRetrieved = check rainierClient->/buildings/[building1.buildingCode].get();
    test:assertEquals(buildingRetrieved, building1);
    check rainierClient.close();
}

@test:Config {
    groups: ["building"]
}
function buildingCreateTest2() returns error? {
    RainierClient rainierClient = check new ();
    
    string[] buildingCodes = check rainierClient->/buildings.post([building2, building3]);

    test:assertEquals(buildingCodes, [building2.buildingCode, building3.buildingCode]);

    Building buildingRetrieved = check rainierClient->/buildings/[building2.buildingCode].get();
    test:assertEquals(buildingRetrieved, building2);

    buildingRetrieved = check rainierClient->/buildings/[building3.buildingCode].get();
    test:assertEquals(buildingRetrieved, building3);

    check rainierClient.close();
}

@test:Config {
    groups: ["building"]
}
function buildingCreateTestNegative() returns error? {
    RainierClient rainierClient = check new ();
    
    string[] building = check rainierClient->/buildings.post([invalidBuilding]);
    io:println("WTHDFD", building);
    // test:assertTrue(building is Error, "Error expected.");
    // if building is Error {
    //     test:assertTrue(building.message().includes("Data truncation: Data too long for column 'buildingCode' at row 1."));
    // } else {
    //     test:assertFail("Error expected.");
    // }
    check rainierClient.close();
}

@test:Config {
    groups: ["building"],
    dependsOn: [buildingCreateTest]
}
function buildingReadOneTest() returns error? {
    RainierClient rainierClient = check new ();

    Building buildingRetrieved = check rainierClient->/buildings/[building1.buildingCode].get();
    test:assertEquals(buildingRetrieved, building1);
    check rainierClient.close();
}

@test:Config {
    groups: ["building"],
    dependsOn: [buildingCreateTest]
}
function buildingReadOneTestNegative() returns error? {
    RainierClient rainierClient = check new ();

    Building|error buildingRetrieved = rainierClient->/buildings/["invalid-building-code"].get();
    test:assertTrue(buildingRetrieved is InvalidKeyError);
    // if buildingRetrieved is InvalidKeyError {
    //     test:assertEquals(buildingRetrieved.message(), "A record does not exist for 'Building' for key \"invalid-building-code\".");
    // } else {
    //     test:assertFail("InvalidKeyError expected.");
    // }
    check rainierClient.close();
}

@test:Config {
    groups: ["building"],
    dependsOn: [buildingCreateTest, buildingCreateTest2]
}
function buildingReadManyTest() returns error? {
    RainierClient rainierClient = check new ();

    stream<Building, error?> buildingStream = rainierClient->/buildings.get();
    Building[] buildings = check from Building building in buildingStream 
        select building;

    test:assertEquals(buildings, [building1, building2, building3]);
    check rainierClient.close();
}

public type BuildingInfo2 record {|
    string city;
    string state;
    string country;
    string postalCode;
    string 'type;
|};

@test:Config {
    groups: ["building", "dependent"],
    dependsOn: [buildingCreateTest, buildingCreateTest2]
}
function buildingReadManyDependentTest() returns error? {
    RainierClient rainierClient = check new ();

    stream<BuildingInfo2, error?> buildingStream = rainierClient->/buildings.get();
    BuildingInfo2[] buildings = check from BuildingInfo2 building in buildingStream 
        select building;

    test:assertEquals(buildings, [
        {city: building1.city, state: building1.state, country: building1.country, postalCode: building1.postalCode, 'type: building1.'type},
        {city: building2.city, state: building2.state, country: building2.country, postalCode: building2.postalCode, 'type: building2.'type},
        {city: building3.city, state: building3.state, country: building3.country, postalCode: building3.postalCode, 'type: building3.'type}
    ]);
    check rainierClient.close();
}

@test:Config {
    groups: ["building"],
    dependsOn: [buildingReadOneTest, buildingReadManyTest, buildingReadManyDependentTest]
}
function buildingUpdateTest() returns error? {
    RainierClient rainierClient = check new ();

    Building building = check rainierClient->/buildings/[building1.buildingCode].put({
        city: "Galle",
        state: "Southern Province",
        postalCode: "10890",
        'type: "owned"
    });

    test:assertEquals(building, updatedBuilding1);

    Building buildingRetrieved = check rainierClient->/buildings/[building1.buildingCode].get();
    test:assertEquals(buildingRetrieved, updatedBuilding1);
    check rainierClient.close();
}

@test:Config {
    groups: ["building"],
    dependsOn: [buildingReadOneTest, buildingReadManyTest, buildingReadManyDependentTest]
}
function buildingUpdateTestNegative1() returns error? {
    RainierClient rainierClient = check new ();

    Building|error building = rainierClient->/buildings/["invalid-building-code"].put({
        city: "Galle",
        state: "Southern Province",
        postalCode: "10890"
    });

    if building is InvalidKeyError {
        test:assertEquals(building.message(), "A record does not exist for 'Building' for key \"invalid-building-code\".");
    } else {
        test:assertFail("InvalidKeyError expected.");
    }
    check rainierClient.close();
}

@test:Config {
    groups: ["building"],
    dependsOn: [buildingReadOneTest, buildingReadManyTest, buildingReadManyDependentTest]
}
function buildingUpdateTestNegative2() returns error? {
    RainierClient rainierClient = check new ();

    Building|error building = rainierClient->/buildings/[building1.buildingCode].put({
        city: "unncessarily-long-city-name-to-force-error-on-update",
        state: "Southern Province",
        postalCode: "10890"
    });

    if building is Error {
        test:assertTrue(building.message().includes("Data truncation: Data too long for column 'city' at row 1."));
    } else {
        test:assertFail("InvalidKeyError expected.");
    }
    check rainierClient.close();
}

@test:Config {
    groups: ["building"],
    dependsOn: [buildingUpdateTest, buildingUpdateTestNegative2]
}
function buildingDeleteTest() returns error? {
    RainierClient rainierClient = check new ();

    Building building = check rainierClient->/buildings/[building1.buildingCode].delete();
    test:assertEquals(building, updatedBuilding1);

    stream<Building, error?> buildingStream = rainierClient->/buildings.get();
    Building[] buildings = check from Building building2 in buildingStream 
        select building2;

    test:assertEquals(buildings, [building2, building3]);
    check rainierClient.close();
}

@test:Config {
    groups: ["building"],
    dependsOn: [buildingDeleteTest]
}
function buildingDeleteTestNegative() returns error? {
    RainierClient rainierClient = check new ();

    Building|error building = rainierClient->/buildings/[building1.buildingCode].delete();

    if building is error {
        test:assertEquals(building.message(), string `A record does not exist for 'Building' for key "${building1.buildingCode}".`);
    } else {
        test:assertFail("InvalidKeyError expected.");
    }
    check rainierClient.close();
}
