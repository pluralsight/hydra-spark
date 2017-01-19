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

import org.apache.avro.Schema;

public class JsonConversionException extends RuntimeException {

    private Object value;

    private String fieldName;

    private Schema schema;

    public JsonConversionException(Object value, String fieldName, Schema schema) {
        super(String.format("Type conversion error for field %s, %s for %s", fieldName, value, schema));
        this.value = value;
        this.fieldName = fieldName;
        this.schema = schema;
    }
}