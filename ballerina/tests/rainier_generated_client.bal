import ballerina/jballerina.java;
import ballerina/time;

const EMPLOYEE = "employees";
const WORKSPACE = "workspaces";
const DEPARTMENT = "departments";
const BUILDING = "buildings";

table<Building> key(buildingCode) buildings = table[];
table<Department> key(deptNo) departments = table[];
table<Workspace> key(workspaceId) workspaces = table[];
table<Employee> key(empNo) employees = table[];

public client class RainierClient {
    *AbstractPersistClient;

    private final map<TableClient> persistClients;

    table<Building> key(buildingCode) buildings = buildings;
    table<Department> key(deptNo) departments = departments;
    table<Workspace> key(workspaceId) workspaces = workspaces;
    table<Employee> key(empNo) employees = employees;


    private final record {|TableMetadata...;|} metadata = {
        [BUILDING]: {
            entityName: "Building",
            keyFields: ["buildingCode"]
        },
        [DEPARTMENT]: {
            entityName: "Department",
            keyFields: ["deptNo"]
        },
        [WORKSPACE]: {
            entityName: "Workspace",
            keyFields: ["workspaceId"]
        },
        [EMPLOYEE]: {
            entityName: "Employee",
            keyFields: ["empNo"]
        }
    };

    public function init() returns Error? {
        self.persistClients = {
            [BUILDING]: check new (self.metadata.get(BUILDING), self.buildings, self.queryBuildings, self.queryOneBuildings),
            [DEPARTMENT]: check new (self.metadata.get(DEPARTMENT), self.departments, self.queryDepartments, self.queryOneDepartments),
            [WORKSPACE]: check new (self.metadata.get(WORKSPACE), self.workspaces, self.queryWorkspaces, self.queryOneWorkspaces),
            [EMPLOYEE]: check new (self.metadata.get(EMPLOYEE), self.employees, self.queryEmployees, self.queryOneEmployees)
        };
    }

    isolated resource function get buildings(BuildingTargetType targetType = <>) returns stream<targetType, Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.datastore.MySQLProcessor",
        name: "query"
    } external;

    isolated resource function get buildings/[string buildingCode](BuildingTargetType targetType = <>) returns targetType|Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.datastore.MySQLProcessor",
        name: "queryOne"
    } external;

    isolated resource function post buildings(BuildingInsert[] data) returns string[]|Error {
        string[] keys = [];
        foreach BuildingInsert value in data {
            if self.buildings.hasKey(value.buildingCode) {
                return <DuplicateKeyError>error("Duplicate key: " + value.buildingCode);
            }
            self.buildings.put(value);
            keys.push(value.buildingCode);
        }
        return keys;
    }

    isolated resource function put buildings/[string buildingCode](BuildingUpdate value) returns Building|Error {
        if !self.buildings.hasKey(buildingCode) {
            return <InvalidKeyError>error("Not found: " + buildingCode);
        }
        Building building = self.buildings.get(buildingCode);
        if value.city != () { building.city = <string>value.city;}
        if value.state != () { building.state = <string>value.state;}
        if value.country != () { building.country = <string>value.country;}
        if value.postalCode != () { building.postalCode = <string>value.postalCode;}
        if value.'type != () { building.'type = <string>value.'type;}
        self.buildings.put(building);
        return building;
    }

    isolated resource function delete buildings/[string buildingCode]() returns Building|Error {
        if !self.buildings.hasKey(buildingCode) {
            return <InvalidKeyError>error("Not found: " + buildingCode);
        }
        return self.buildings.remove(buildingCode);
    }

    isolated resource function get departments(DepartmentTargetType targetType = <>) returns stream<targetType, Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.datastore.MySQLProcessor",
        name: "query"
    } external;

    isolated resource function get departments/[string deptNo](DepartmentTargetType targetType = <>) returns targetType|Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.datastore.MySQLProcessor",
        name: "queryOne"
    } external;

    isolated resource function post departments(DepartmentInsert[] data) returns string[]|Error {
        string[] keys = [];
        foreach DepartmentInsert value in data {
            if self.departments.hasKey(value.deptNo) {
                return <DuplicateKeyError>error("Duplicate key: " + value.deptNo);
            }
            self.departments.put(value);
            keys.push(value.deptNo);
        }
        return keys;
    }
    
    isolated resource function put departments/[string deptNo](DepartmentUpdate value) returns Department|Error {
        if !self.departments.hasKey(deptNo) {
            return <InvalidKeyError>error("Not found: " + deptNo);
        }
        Department department = self.departments.get(deptNo);
        if value.deptName != () { department.deptName = <string>value.deptName;}
        self.departments.put(department);
        return department;
    }

    isolated resource function delete departments/[string deptNo]() returns Department|Error {
        if !self.departments.hasKey(deptNo) {
            return <InvalidKeyError>error("Not found: " + deptNo);
        }
        return self.departments.remove(deptNo);
    }

    isolated resource function get workspaces(WorkspaceTargetType targetType = <>) returns stream<targetType, Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.datastore.MySQLProcessor",
        name: "query"
    } external;

    isolated resource function get workspaces/[string workspaceId](WorkspaceTargetType targetType = <>) returns targetType|Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.datastore.MySQLProcessor",
        name: "queryOne"
    } external;

    isolated resource function post workspaces(WorkspaceInsert[] data) returns string[]|Error {
        string[] keys = [];
        foreach WorkspaceInsert value in data {
            if self.workspaces.hasKey(value.workspaceId) {
                return <DuplicateKeyError>error("Duplicate key: " + value.workspaceId);
            }
            self.workspaces.put(value);
            keys.push(value.workspaceId);
        }
        return keys;
    }

    isolated resource function put workspaces/[string workspaceId](WorkspaceUpdate value) returns Workspace|Error {
        if !self.workspaces.hasKey(workspaceId) {
            return <InvalidKeyError>error("Not found: " + workspaceId);
        }
        Workspace workspace = self.workspaces.get(workspaceId);
        if value.locationBuildingCode != () { workspace.locationBuildingCode = <string>value.locationBuildingCode;}
        if value.workspaceType != () { workspace.workspaceType = <string>value.workspaceType;}
        self.workspaces.put(workspace);
        return workspace;
    }

    isolated resource function delete workspaces/[string workspaceId]() returns Workspace|Error {
        if !self.workspaces.hasKey(workspaceId) {
            return <InvalidKeyError>error("Not found: " + workspaceId);
        }
        return self.workspaces.remove(workspaceId);
    }

    isolated resource function get employees(EmployeeTargetType targetType = <>) returns stream<targetType, Error?> = @java:Method {
        'class: "io.ballerina.stdlib.persist.datastore.MySQLProcessor",
        name: "query"
    } external;

    isolated resource function get employees/[string empNo](EmployeeTargetType targetType = <>) returns targetType|Error = @java:Method {
        'class: "io.ballerina.stdlib.persist.datastore.MySQLProcessor",
        name: "queryOne"
    } external;

    isolated resource function post employees(EmployeeInsert[] data) returns string[]|Error {
        string[] keys = [];
        foreach EmployeeInsert value in data {
            if self.employees.hasKey(value.empNo) {
                return <DuplicateKeyError>error("Duplicate key: " + value.empNo);
            }
            self.employees.put(value);
            keys.push(value.empNo);
        }
        return keys;
    }

    isolated resource function put employees/[string empNo](EmployeeUpdate value) returns Employee|Error {
        if !self.employees.hasKey(empNo) {
            return <InvalidKeyError>error("Not found: " + empNo);
        }
        Employee employee = self.employees.get(empNo);
        if value.firstName != () { employee.firstName = <string>value.firstName;}
        if value.lastName != () { employee.lastName = <string>value.lastName;}
        if value.birthDate != () { employee.birthDate = <time:Date>value.birthDate;}
        if value.departmentDeptNo != () { employee.departmentDeptNo = <string>value.departmentDeptNo;}
        if value.gender != () { employee.gender = <string>value.gender;}
        if value.hireDate != () { employee.hireDate = <time:Date>value.hireDate;}
        self.employees.put(employee);
        return employee;   
    }

    isolated resource function delete employees/[string empNo]() returns Employee|Error {
        if !self.employees.hasKey(empNo) {
            return <InvalidKeyError>error("Not found: " + empNo);
        }
        return self.employees.remove(empNo);
    }

    public function close() returns Error? {
        return ();
    }   


    public isolated function queryEmployees() returns stream<record{}, Error?> {
        return from record{} 'object in self.employees
            outer join var department in self.departments
            on 'object.departmentDeptNo equals department?.deptNo
            outer join var workspace in self.workspaces
            on 'object.workspaceWorkspaceId equals workspace?.workspaceId
            select {
                ...'object,
                "department": department,
                "workspace": workspace
            };
    }

    public isolated function queryOneEmployees(anydata key) returns record {}|InvalidKeyError {
        from record{} 'object in self.employees
            where self.persistClients.get(EMPLOYEE).getKey('object) == key
            outer join var department in self.departments
            on 'object.departmentDeptNo equals department?.deptNo
            outer join var workspace in self.workspaces
            on 'object.workspaceWorkspaceId equals workspace?.workspaceId
            do {
                return {
                    ...'object,
                    "department": department,
                    "workspace": workspace
                };
            };
        return <InvalidKeyError>error("Invalid key: " + key.toString());
    }

    public isolated function queryBuildings() returns stream<record{}, Error?> {
        return from record{} 'object in self.buildings
            select {
                ...'object
            };
    }

    public isolated function queryOneBuildings(anydata key) returns record{}|InvalidKeyError {
        from record{} 'object in self.buildings
            where self.persistClients.get(BUILDING).getKey('object) == key
            do {
                return {
                    ...'object
                };
            };
        return <InvalidKeyError>error("Invalid key: " + key.toString());
    }

    public isolated function queryDepartments() returns stream<record{}, Error?> {
        return from record{} 'object in self.departments
            select {
                ...'object
            };
    }

    public isolated function queryOneDepartments(anydata key) returns record{}|InvalidKeyError {
        from record{} 'object in self.departments
            where self.persistClients.get(DEPARTMENT).getKey('object) == key
            do {
                return {
                    ...'object
                };
            };
        return <InvalidKeyError>error("Invalid key: " + key.toString());
    }
    
    public isolated function queryWorkspaces() returns stream<record{}, Error?> {
        return from record{} 'object in self.workspaces
            outer join var location in self.buildings
            on 'object.locationBuildingCode equals location?.buildingCode
            select {
                ...'object,
                "location": location
            };
    }

    public isolated function queryOneWorkspaces(anydata key) returns record{}|InvalidKeyError {
        from record{} 'object in self.workspaces
            where self.persistClients.get(WORKSPACE).getKey('object) == key
            outer join var location in self.buildings
            on 'object.locationBuildingCode equals location?.buildingCode
            do {
                return {
                    ...'object,
                    "location": location
                };
            };
        return <InvalidKeyError>error("Invalid key: " + key.toString());
    }
}

