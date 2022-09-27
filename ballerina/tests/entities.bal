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

import ballerina/time;

@Entity {
    key: ["needId"],
    tableName: "MedicalNeeds"
}
public type MedicalNeed record {|
    @AutoIncrement
    readonly int needId = -1;

    int itemId;
    int beneficiaryId;
    time:Civil period;
    string urgency;
    int quantity;
|};

@Entity {
    key: ["itemId"],
    tableName: "MedicalItems"
}
public type MedicalItem record {|
    readonly int itemId;
    string name;
    string 'type;
    string unit;
|};

@Entity {
    key: ["hospitalCode", "departmentId"],
    tableName: "Departments"
}
public type Department record {|
    string hospitalCode;
    int departmentId;
    string name;
|};

// One-to-one relation
@Entity { 
    key: ["id"],
    tableName: "Users"
}
public type User record  {|	
    readonly int id;
    string name;
    Profile profile?;
|};
 
@Entity {
    key: ["id"],
    tableName: "Profiles"
}
public type Profile record  {|
    readonly int id;
    string name;
    @Relation {keyColumns: ["userId"], reference: ["id"]}
    User user?;
|};

@Entity { 
    key: ["id"],
    tableName: "MultipleAssociations"
}
public type MultipleAssociations record {|
    readonly int id;
    string name;

    @Relation {keyColumns: ["profileId"], reference: ["id"]}
    Profile profile?;

    @Relation {keyColumns: ["userId"], reference: ["id"]}
    User user?;
|};

// One-to-many relation
@Entity { 
    key: ["id"],
    tableName: "Companies"
}
public type Company record  {|	
    readonly int id;
    string name;
    Employee[] employees?;
|};

@Entity { 
    key: ["id"],
    tableName: "Employees" 
}
public type Employee record  {|	
    readonly int id;
    string name;

    @Relation {keyColumns: ["companyId"], reference: ["id"]}
    Company company?;
|};

// many-to-many relation
@Entity { 
    key: ["id"],
    tableName: "Teachers"
}
public type Teacher record  {|	
    readonly int id;
    string name;
    Student[] students?;
|};

@Entity { 
    key: ["id"],
    tableName: "Students"
}
public type Student record  {|	
    readonly int id;
    string name;
    Teacher[] teachers?;
|};
