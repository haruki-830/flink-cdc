/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.common.sink;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;

import java.util.Optional;

/** A {@link MetadataApplier} that can read the current schema of a target table. */
@Experimental
public interface SchemaAwareMetadataApplier extends MetadataApplier {

    /**
     * Returns the current schema of the target table, or {@link Optional#empty()} if the table does
     * not exist.
     */
    Optional<Schema> getTargetTableSchema(TableId tableId) throws SchemaEvolveException;

    /**
     * Converts a pipeline type to the CDC representation of the physical type that this applier
     * will create in the target system.
     *
     * <p>Implementations whose physical type mapping is equivalent to the pipeline type can use the
     * identity implementation. This method must not query or modify the target table.
     */
    default DataType normalizeToTargetDataType(
            TableId tableId, String columnName, DataType pipelineDataType) {
        return pipelineDataType;
    }
}
