/*
 * Copyright (C) 2017 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pluralsight.hydra.common.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ClassUtil;
import com.google.common.collect.*;
import hydra.spark.avro.AvroUtils;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.avro.Schema.Field;
import static org.apache.avro.Schema.Type;

public class JsonConverter<T extends GenericRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonConverter.class);

    private static final Set<Type> SUPPORTED_TYPES = ImmutableSet.of(Type.RECORD, Type.ARRAY, Type.MAP, Type.INT,
            Type.LONG, Type.BOOLEAN, Type.FLOAT, Type.DOUBLE, Type.STRING, Type.ENUM);

    private final Class<T> typeClass;

    private final ObjectMapper mapper = new ObjectMapper();

    private final Schema baseSchema;

    private ConversionStats conversionStats = new ConversionStats();

    public JsonConverter(Schema schema) {
        this(null, schema);
    }

    public JsonConverter(Class<T> clazz, Schema schema) {
        this.typeClass = clazz;
        this.baseSchema = validate(schema, true);
        assert SchemaBuilder.builder().booleanBuilder().endBoolean() != null;
    }

    private Schema validate(Schema schema, boolean mustBeRecord) {
        if (!mustBeRecord) {
            if (!SUPPORTED_TYPES.contains(schema.getType())) {
                throw new IllegalArgumentException("Unsupported type: " + schema.getType());
            }
            if (schema.getType() != Type.RECORD) {
                return schema;
            }
        }
        for (Field field : schema.getFields()) {
            Schema fs = field.schema();
            if (isNullableSchema(fs)) {
                fs = getNonNull(fs);
            }
            Type type = fs.getType();
            if (!SUPPORTED_TYPES.contains(type)) {
                throw new IllegalArgumentException(String.format("Unsupported type '%s' for field '%s'", type
                        .toString(), field.name()));
            }
            switch (type) {
                case RECORD:
                    validate(fs, true);
                    break;
                case MAP:
                    //trying to support options in map values
                    Schema fsVal = fs.getValueType();
                    if (isNullableSchema(fsVal)) {
                        fsVal = getNonNull(fsVal);
                    }
                    validate(fsVal, false);
                    break;
                case ARRAY:
                    validate(fs.getElementType(), false);
                default:
                    break;
            }
        }
        return schema;
    }

    public T convert(String json) throws IOException {
        try {
            return (T) convert(mapper.readValue(json, Map.class), baseSchema);
        } catch (IOException e) {
            throw new IOException("Failed to parse as Json: " + json + "\n\n" + e.getMessage());
        }
    }

    private T convert(Map<String, Object> raw, Schema schema) throws IOException {
        GenericRecord result = typeClass == null ? new GenericData.Record(schema) : ClassUtil.createInstance
                (typeClass, false);
        Map<String, Object> clean = cleanAvro(raw);
        Set<String> usedFields = Sets.newHashSet();
        Set<String> missingFields = Sets.newHashSet();
        for (Field f : schema.getFields()) {
            String name = f.name();
            Object rawValue = clean.get(name);
            //try one more time fixi
            if (rawValue != null) {
                result.put(f.pos(), typeConvert(rawValue, name, f.schema()));
                usedFields.add(name);
            } else {
                missingFields.add(name);
                JsonNode defaultValue = f.defaultValue();
                if (defaultValue == null || defaultValue.isNull()) {
                    if (isNullableSchema(f.schema())) {
                        result.put(f.pos(), null);
                    } else {
                        throw new IllegalArgumentException("No default value provided for non-nullable field: " + f
                                .name());
                    }
                } else {
                    Schema fieldSchema = f.schema();
                    if (isNullableSchema(fieldSchema)) {
                        fieldSchema = getNonNull(fieldSchema);
                    }
                    Object value;
                    switch (fieldSchema.getType()) {
                        case BOOLEAN:
                            value = defaultValue.asBoolean();
                            break;
                        case DOUBLE:
                            value = defaultValue.asDouble();
                            break;
                        case FLOAT:
                            value = (float) defaultValue.asDouble();
                            break;
                        case INT:
                            value = defaultValue.asInt();
                            break;
                        case LONG:
                            value = defaultValue.asLong();
                            break;
                        case STRING:
                            value = defaultValue.asText();
                            break;
                        case MAP:
                            Map<String, Object> fieldMap = mapper.readValue(defaultValue.asText(), Map.class);
                            Map<String, Object> mvalue = Maps.newHashMap();
                            for (Map.Entry<String, Object> e : fieldMap.entrySet()) {
                                mvalue.put(e.getKey(), typeConvert(e.getValue(), name, fieldSchema.getValueType()));
                            }
                            value = mvalue;
                            break;
                        case ARRAY:
                            List fieldArray = mapper.readValue(defaultValue.asText(), List.class);
                            List lvalue = Lists.newArrayList();
                            for (Object elem : fieldArray) {
                                lvalue.add(typeConvert(elem, name, fieldSchema.getElementType()));
                            }
                            value = lvalue;
                            break;
                        case RECORD:
                            Map<String, Object> fieldRec = mapper.readValue(defaultValue.asText(), Map.class);
                            value = convert(fieldRec, fieldSchema);
                            break;
                        default:
                            throw new IllegalArgumentException("JsonConverter cannot handle type: " + fieldSchema
                                    .getType());
                    }
                    result.put(f.pos(), value);
                }
            }
        }

        if (usedFields.size() < raw.size()) {
            LOG.warn("Ignoring unused JSON fields: " + Sets.difference(raw.keySet(), usedFields));
        }

        conversionStats.reportMissingFields(missingFields);

        return (T) result;
    }

    private Object typeConvert(Object value, String name, Schema schema) throws IOException {
        if (isNullableSchema(schema)) {
            if (value == null) {
                return null;
            } else {
                schema = getNonNull(schema);
            }
        } else if (value == null) {
            throw new JsonConversionException(value, name, schema);
        }

        switch (schema.getType()) {
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                } else if (value instanceof String) {
                    return Boolean.valueOf((String) value);
                } else if (value instanceof Number) {
                    return ((Number) value).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
                }
                break;
            case DOUBLE:
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                } else if (value instanceof String) {
                    return Double.valueOf((String) value);
                }
                break;
            case FLOAT:
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                } else if (value instanceof String) {
                    return Float.valueOf((String) value);
                }
                break;
            case INT:
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                } else if (value instanceof String) {
                    return Integer.valueOf((String) value);
                }
                break;
            case LONG:
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                } else if (value instanceof String) {
                    return Long.valueOf((String) value);
                }
                break;
            case STRING:
                return value.toString();
            case ENUM:
                boolean valid = schema.getEnumSymbols().contains(value);
                if (!valid)
                    throw new IllegalArgumentException(value + " is not a valid symbol. Possible values are: " +
                            schema.getEnumSymbols() + ".");
                return value;
//                try {
//                    Class<Enum> enumType = (Class<Enum>) Class.forName(schema.getFullName());
//                    return Enum.valueOf(enumType, value.toString());
//                } catch (ClassNotFoundException e) {
//                    throw new RuntimeException(e);
//                }
            case RECORD:
                return convert((Map<String, Object>) value, schema);
            case ARRAY:
                Schema elementSchema = schema.getElementType();
                List listRes = new ArrayList();
                for (Object v : (List) value) {
                    listRes.add(typeConvert(v, name, elementSchema));
                }
                return listRes;
            case MAP:
                Schema valueSchema = schema.getValueType();
                Map<String, Object> mapRes = Maps.newHashMap();
                for (Map.Entry<String, Object> v : ((Map<String, Object>) value).entrySet()) {
                    mapRes.put(v.getKey(), typeConvert(v.getValue(), name, valueSchema));
                }
                return mapRes;
            default:
                throw new IllegalArgumentException("Cannot handle type: " + schema.getType());
        }
        throw new JsonConversionException(value, name, schema);
    }

    private boolean isNullableSchema(Schema schema) {
        return schema.getType().equals(Type.UNION) && schema.getTypes().size() == 2 && (schema.getTypes().get(0)
                .getType().equals(Type.NULL) || schema.getTypes().get(1).getType().equals(Type.NULL));
    }

    private Schema getNonNull(Schema schema) {
        List<Schema> types = schema.getTypes();
        return types.get(0).getType().equals(Type.NULL) ? types.get(1) : types.get(0);
    }


    public ConversionStats getConversionStats() {
        return conversionStats;
    }

    public class ConversionStats {
        private Multiset<String> missingFields = HashMultiset.create();

        public void reportMissingFields(Set<String> missingFields) {
            if (!missingFields.isEmpty()) {
                for (String name : missingFields) {
                    this.missingFields.add(name);
                }
            }
        }

        public int missingFieldsCount() {
            return missingFields.size();
        }

        public Set<String> getMissingFields() {
            return missingFields.elementSet();
        }

        public int getMissingFieldCount(String name) {
            return missingFields.count(name);
        }
    }


    private Map cleanAvro(Map<String, Object> oldRaw) {
        Map<String, Object> newMap = Maps.newHashMap();
        try {
            oldRaw.forEach((entry, value) -> newMap.put(AvroUtils.cleanName(entry), value));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return newMap;
    }

}