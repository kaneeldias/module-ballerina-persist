/*
 *  Copyright (c) 2023, WSO2 LLC. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.ballerina.stdlib.persist;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.Module;
import io.ballerina.runtime.api.PredefinedTypes;
import io.ballerina.runtime.api.TypeTags;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.Field;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.ReferenceType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BFuture;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.ballerina.runtime.api.utils.StringUtils.fromString;

/**
 * This class has the utility methods required for the Persist module.
 *
 * @since 0.1.0
 */
public class Utils {
    private static final List<String> KNOWN_RECORD_TYPES = Arrays.asList(
            Constants.TimeTypes.CIVIL, Constants.TimeTypes.DATE_RECORD, Constants.TimeTypes.TIME_RECORD);


    private Utils() {
    }

    static BString getEntity(Environment env) {
        String entity = env.getFunctionName().split("\\$")[2];
        return fromString(entity);
    }

    static BObject getPersistClient(BObject client, BString entity) {
        BMap<?, ?> persistClients = (BMap<?, ?>) client.get(Constants.PERSIST_CLIENTS);
        return (BObject) persistClients.get(entity);
    }

    static BArray[] getFieldsIncludesAndTypeDescriptions(RecordType recordType) {
        ArrayType stringArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_STRING);

        //TODO: use PREDEFINED TYPE.TYPE_TYPEDESC once NPE issue is resolved
        ArrayType typeDescriptionArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_ANY);
        BArray fieldsArray = ValueCreator.createArrayValue(stringArrayType);
        BArray includeArray = ValueCreator.createArrayValue(stringArrayType);
        BArray typeDescriptionArray = ValueCreator.createArrayValue(typeDescriptionArrayType);

        Map<String, Field> fieldsMap = recordType.getFields();
        for (Field field : fieldsMap.values()) {
            Type type = field.getFieldType();

            boolean arrayType = false;
            if (type.getTag() == TypeTags.ARRAY_TAG) {
                type = ((ArrayType) type).getElementType();
                arrayType = true;
            }

            if ((type.getTag() == TypeTags.RECORD_TYPE_TAG || type.getTag() == TypeTags.TYPE_REFERENCED_TYPE_TAG) &&
                    !isKnownRecordType(type)) {
                String innerFieldName = field.getFieldName();
                includeArray.append(fromString(innerFieldName));

                BArray innerFieldsArray = getInnerFieldsArray(type);
                for (int i = 0; i < innerFieldsArray.size(); i++) {
                    if (arrayType) {
                        fieldsArray.append(fromString(innerFieldName + "[]." + innerFieldsArray.get(i).toString()));
                    } else {
                        fieldsArray.append(fromString(innerFieldName + "." + innerFieldsArray.get(i).toString()));
                    }
                }

                typeDescriptionArray.append(ValueCreator.createTypedescValue(type));
            } else {
                fieldsArray.append(fromString(field.getFieldName()));
            }
        }

        return new BArray[]{fieldsArray, includeArray, typeDescriptionArray};
    }

    static BTypedesc getRowType(RecordType recordType) {
        Map<String, Field> fieldsMap = new HashMap<>();

        for (Field field : recordType.getFields().values()) {
            Type type = field.getFieldType();

            if (type.getTag() == TypeTags.ARRAY_TAG) {
                type = ((ArrayType) type).getElementType();

                if ((type.getTag() == TypeTags.RECORD_TYPE_TAG || type.getTag() == TypeTags.TYPE_REFERENCED_TYPE_TAG) &&
                        !isKnownRecordType(type)) {
                    fieldsMap.put(field.getFieldName(), TypeCreator.createField(PredefinedTypes.TYPE_STRING,
                            field.getFieldName(), field.getFlags()));
                    continue;
                }
            }

            fieldsMap.put(field.getFieldName(), field);
        }

        Module ballerinaAnnotation = new Module("ballerina", "lang.annotations", "0.0.0");
        Type customRecordType = TypeCreator.createRecordType("$stream$anon$constraint$", ballerinaAnnotation,
                recordType.getFlags(), fieldsMap, PredefinedTypes.TYPE_ANYDATA, true, recordType.getTypeFlags());
        return ValueCreator.createTypedescValue(customRecordType);
    }

    private static BArray getInnerFieldsArray(Type type) {
        RecordType recordType;
        if (type.getTag() == TypeTags.RECORD_TYPE_TAG) {
            recordType = (RecordType) type;
        } else {
            recordType = (RecordType) ((ReferenceType) type).getReferredType();
        }

        ArrayType stringArrayType = TypeCreator.createArrayType(PredefinedTypes.TYPE_STRING);
        BArray fieldsArray = ValueCreator.createArrayValue(stringArrayType);
        Map<String, Field> fieldsMap = recordType.getFields();
        for (Field field : fieldsMap.values()) {
            fieldsArray.append(fromString(field.getFieldName()));
        }

        return fieldsArray;
    }

    static Object getFutureResult(BFuture future) {
        while (!future.isDone()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                // ignore
            }
        }
        return future.getResult();
    }

    static boolean isKnownRecordType(Type ballerinaType) {
        return KNOWN_RECORD_TYPES.contains(getBTypeName(ballerinaType));
    }

    private static String getBTypeName(Type ballerinaType) {
        if (ballerinaType.getName() == null || ballerinaType.getName().equals("")) {
            return ballerinaType.toString();
        }
        return ballerinaType.getName();
    }

    public static BArray convertToArray(BTypedesc recordType, BArray arr) {
        ArrayType array = TypeCreator.createArrayType(recordType.getDescribingType());
        BArray returnArray = ValueCreator.createArrayValue(array);
        for (Object element : arr.getValues()) {
            if (element == null) {
                break;
            }
            returnArray.append(element);
        }
        return returnArray;
    }

}
