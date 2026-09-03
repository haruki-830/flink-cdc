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

package org.apache.flink.cdc.runtime.operators.schema.common;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.SchemaAwareMetadataApplier;
import org.apache.flink.cdc.common.types.BinaryType;
import org.apache.flink.cdc.common.types.CharType;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypeFamily;
import org.apache.flink.cdc.common.types.DataTypeRoot;
import org.apache.flink.cdc.common.types.DecimalType;
import org.apache.flink.cdc.common.types.LocalZonedTimestampType;
import org.apache.flink.cdc.common.types.TimeType;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.types.VarBinaryType;
import org.apache.flink.cdc.common.types.VarCharType;
import org.apache.flink.cdc.common.types.ZonedTimestampType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** Coordinates best-effort schema repairs for an existing target table. */
@Internal
public class SchemaReconciler {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaReconciler.class);

    private final SchemaAwareMetadataApplier metadataApplier;
    private final SchemaChangeBehavior schemaChangeBehavior;

    public SchemaReconciler(
            SchemaAwareMetadataApplier metadataApplier, SchemaChangeBehavior schemaChangeBehavior) {
        this.metadataApplier = metadataApplier;
        this.schemaChangeBehavior = schemaChangeBehavior;
    }

    /** Tries safe repairs without imposing new compatibility failures. */
    public ReconcileResult reconcile(CreateTableEvent createTableEvent) {
        try {
            return reconcileInternal(createTableEvent);
        } catch (Exception e) {
            LOG.warn(
                    "Unexpected error while reconciling target table {}. Delegating schema handling to the sink.",
                    createTableEvent.tableId(),
                    e);
            return ReconcileResult.DELEGATE_TO_SINK;
        }
    }

    private ReconcileResult reconcileInternal(CreateTableEvent createTableEvent) throws Exception {
        Optional<Schema> targetSchema = queryTargetSchema(createTableEvent.tableId());
        if (!targetSchema.isPresent()) {
            return ReconcileResult.NO_ACTION;
        }

        Schema pipelineSchema = createTableEvent.getSchema();
        Schema currentTargetSchema = targetSchema.get();
        Map<String, Column> targetColumns = indexColumns(currentTargetSchema);
        Set<String> keyColumns = getKeyColumns(pipelineSchema, currentTargetSchema);

        List<Column> columnsToAdd = new ArrayList<>();
        Map<String, DataType> columnsToWiden = new LinkedHashMap<>();
        Map<String, DataType> expectedPipelineTypes = new HashMap<>();
        boolean delegateToSink = false;

        for (Column pipelineColumn : pipelineSchema.getColumns()) {
            String columnName = pipelineColumn.getName();
            Column targetColumn = targetColumns.get(columnName);
            if (targetColumn == null) {
                if (pipelineColumn.isPhysical() && !keyColumns.contains(columnName)) {
                    columnsToAdd.add(pipelineColumn.copy(pipelineColumn.getType().nullable()));
                } else {
                    delegateToSink = true;
                    LOG.info(
                            "Target table {} is missing special column {}. Delegating this difference to the sink.",
                            createTableEvent.tableId(),
                            columnName);
                }
                continue;
            }

            DataType normalizedPipelineType =
                    normalizeType(createTableEvent.tableId(), columnName, pipelineColumn.getType())
                            .nullable();
            DataType targetType = targetColumn.getType().nullable();

            if (pipelineColumn.getType().isNullable() && !targetColumn.getType().isNullable()) {
                delegateToSink = true;
                LOG.info(
                        "Target column {}.{} is NOT NULL while the pipeline column is nullable. Delegating this difference to the sink.",
                        createTableEvent.tableId(),
                        columnName);
            }

            if (canContain(targetType, normalizedPipelineType)) {
                continue;
            }
            if (keyColumns.contains(columnName)) {
                delegateToSink = true;
                LOG.info(
                        "Target key column {}.{} cannot contain pipeline type {}. Delegating this difference to the sink.",
                        createTableEvent.tableId(),
                        columnName,
                        normalizedPipelineType);
                continue;
            }

            Optional<DataType> widenedType = getSafeWidenedType(targetType, normalizedPipelineType);
            if (!widenedType.isPresent()) {
                delegateToSink = true;
                LOG.info(
                        "Target column {}.{} with type {} cannot safely contain pipeline type {}. Delegating this difference to the sink.",
                        createTableEvent.tableId(),
                        columnName,
                        targetType,
                        normalizedPipelineType);
                continue;
            }

            DataType normalizedWidenedType =
                    normalizeType(createTableEvent.tableId(), columnName, widenedType.get())
                            .nullable();
            if (!canContain(normalizedWidenedType, targetType)
                    || !canContain(normalizedWidenedType, normalizedPipelineType)) {
                delegateToSink = true;
                LOG.info(
                        "Target system normalizes proposed type {} for {}.{} to {}, which is not a safe widening. Delegating this difference to the sink.",
                        widenedType.get(),
                        createTableEvent.tableId(),
                        columnName,
                        normalizedWidenedType);
                continue;
            }

            columnsToWiden.put(
                    columnName, widenedType.get().copy(targetColumn.getType().isNullable()));
            expectedPipelineTypes.put(columnName, normalizedPipelineType);
        }

        List<Column> addedColumns = new ArrayList<>();
        Map<String, DataType> widenedColumns = new LinkedHashMap<>();

        if (!columnsToAdd.isEmpty()) {
            if (supportsSchemaEvolutionType(SchemaChangeEventType.ADD_COLUMN)) {
                AddColumnEvent addColumnEvent =
                        new AddColumnEvent(
                                createTableEvent.tableId(),
                                columnsToAdd.stream()
                                        .map(AddColumnEvent.ColumnWithPosition::new)
                                        .collect(Collectors.toList()));
                if (applySchemaChange(addColumnEvent, columnsToAdd)) {
                    addedColumns.addAll(columnsToAdd);
                } else {
                    delegateToSink = true;
                }
            } else {
                delegateToSink = true;
                LOG.info(
                        "Target table {} is missing columns {}, but ADD_COLUMN is not enabled or supported. Delegating this difference to the sink.",
                        createTableEvent.tableId(),
                        getColumnNames(columnsToAdd));
            }
        }

        if (!columnsToWiden.isEmpty()) {
            if (supportsSchemaEvolutionType(SchemaChangeEventType.ALTER_COLUMN_TYPE)) {
                AlterColumnTypeEvent alterColumnTypeEvent =
                        new AlterColumnTypeEvent(
                                createTableEvent.tableId(),
                                columnsToWiden,
                                columnsToWiden.keySet().stream()
                                        .collect(
                                                Collectors.toMap(
                                                        columnName -> columnName,
                                                        columnName ->
                                                                targetColumns
                                                                        .get(columnName)
                                                                        .getType())));
                if (applySchemaChange(alterColumnTypeEvent, columnsToWiden)) {
                    widenedColumns.putAll(columnsToWiden);
                } else {
                    delegateToSink = true;
                }
            } else {
                delegateToSink = true;
                LOG.info(
                        "Target table {} has narrow columns {}, but ALTER_COLUMN_TYPE is not enabled or supported. Delegating this difference to the sink.",
                        createTableEvent.tableId(),
                        columnsToWiden.keySet());
            }
        }

        if (addedColumns.isEmpty() && widenedColumns.isEmpty()) {
            return delegateToSink ? ReconcileResult.DELEGATE_TO_SINK : ReconcileResult.NO_ACTION;
        }

        Optional<Schema> refreshedTargetSchema = queryTargetSchema(createTableEvent.tableId());
        if (!refreshedTargetSchema.isPresent()) {
            LOG.warn(
                    "Target table {} was unavailable after applying reconciliation. Delegating schema handling to the sink.",
                    createTableEvent.tableId());
            return ReconcileResult.DELEGATE_TO_SINK;
        }

        Map<String, Column> refreshedColumns = indexColumns(refreshedTargetSchema.get());
        List<String> columnsStillMissing =
                addedColumns.stream()
                        .map(Column::getName)
                        .filter(columnName -> !refreshedColumns.containsKey(columnName))
                        .collect(Collectors.toList());
        if (!columnsStillMissing.isEmpty()) {
            delegateToSink = true;
            LOG.warn(
                    "Target table {} is still missing columns {} after reconciliation. Delegating this difference to the sink.",
                    createTableEvent.tableId(),
                    columnsStillMissing);
        }

        List<String> columnsStillNarrow =
                widenedColumns.keySet().stream()
                        .filter(
                                columnName -> {
                                    Column refreshedColumn = refreshedColumns.get(columnName);
                                    return refreshedColumn == null
                                            || !canContain(
                                                    refreshedColumn.getType().nullable(),
                                                    expectedPipelineTypes.get(columnName));
                                })
                        .collect(Collectors.toList());
        if (!columnsStillNarrow.isEmpty()) {
            delegateToSink = true;
            LOG.warn(
                    "Target table {} still has narrow columns {} after reconciliation. Delegating this difference to the sink.",
                    createTableEvent.tableId(),
                    columnsStillNarrow);
        }

        if (delegateToSink) {
            return ReconcileResult.DELEGATE_TO_SINK;
        }

        LOG.info(
                "Reconciled existing target table {} by adding columns {} and widening columns {}.",
                createTableEvent.tableId(),
                getColumnNames(addedColumns),
                widenedColumns);
        return ReconcileResult.REPAIRED;
    }

    private Optional<Schema> queryTargetSchema(TableId tableId) throws Exception {
        try {
            return metadataApplier.getTargetTableSchema(tableId);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to query schema of target table {}. Delegating schema handling to the sink.",
                    tableId,
                    e);
            throw e;
        }
    }

    private DataType normalizeType(TableId tableId, String columnName, DataType pipelineType) {
        DataType normalizedType =
                metadataApplier.normalizeToTargetDataType(tableId, columnName, pipelineType);
        if (normalizedType == null) {
            throw new IllegalStateException(
                    String.format(
                            "Metadata applier returned a null normalized type for %s.%s",
                            tableId, columnName));
        }
        return normalizedType;
    }

    private boolean applySchemaChange(SchemaChangeEvent event, Object changes) {
        try {
            metadataApplier.applySchemaChange(event);
            return true;
        } catch (Exception e) {
            LOG.warn(
                    "Failed to apply reconciliation change {} to target table {}. Delegating schema handling to the sink.",
                    changes,
                    event.tableId(),
                    e);
            return false;
        }
    }

    private boolean supportsSchemaEvolutionType(SchemaChangeEventType eventType) {
        if (schemaChangeBehavior == SchemaChangeBehavior.IGNORE
                || schemaChangeBehavior == SchemaChangeBehavior.EXCEPTION) {
            return false;
        }
        try {
            return metadataApplier.acceptsSchemaEvolutionType(eventType)
                    && metadataApplier.getSupportedSchemaEvolutionTypes().contains(eventType);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to determine whether {} is enabled and supported. Delegating schema handling to the sink.",
                    eventType,
                    e);
            return false;
        }
    }

    private static Map<String, Column> indexColumns(Schema schema) {
        return schema.getColumns().stream()
                .collect(Collectors.toMap(Column::getName, column -> column));
    }

    private static Set<String> getKeyColumns(Schema pipelineSchema, Schema targetSchema) {
        Set<String> keyColumns = new HashSet<>();
        keyColumns.addAll(pipelineSchema.primaryKeys());
        keyColumns.addAll(pipelineSchema.partitionKeys());
        keyColumns.addAll(targetSchema.primaryKeys());
        keyColumns.addAll(targetSchema.partitionKeys());
        return keyColumns;
    }

    private static List<String> getColumnNames(List<Column> columns) {
        return columns.stream().map(Column::getName).collect(Collectors.toList());
    }

    private static boolean canContain(DataType targetType, DataType sourceType) {
        targetType = targetType.notNull();
        sourceType = sourceType.notNull();
        if (targetType.equals(sourceType)) {
            return true;
        }
        if (targetType.is(DataTypeFamily.INTEGER_NUMERIC)
                && sourceType.is(DataTypeFamily.INTEGER_NUMERIC)) {
            return integerRank(targetType) >= integerRank(sourceType);
        }
        if (targetType.is(DataTypeFamily.APPROXIMATE_NUMERIC)
                && sourceType.is(DataTypeFamily.APPROXIMATE_NUMERIC)) {
            return approximateNumericRank(targetType) >= approximateNumericRank(sourceType);
        }
        if (targetType instanceof DecimalType && sourceType instanceof DecimalType) {
            DecimalType targetDecimal = (DecimalType) targetType;
            DecimalType sourceDecimal = (DecimalType) sourceType;
            return targetDecimal.getScale() >= sourceDecimal.getScale()
                    && targetDecimal.getPrecision() - targetDecimal.getScale()
                            >= sourceDecimal.getPrecision() - sourceDecimal.getScale();
        }
        if (targetType.is(DataTypeFamily.CHARACTER_STRING)
                && sourceType.is(DataTypeFamily.CHARACTER_STRING)) {
            return (targetType instanceof VarCharType
                            && getCharacterLength(targetType) >= getCharacterLength(sourceType))
                    || (targetType instanceof CharType
                            && sourceType instanceof CharType
                            && getCharacterLength(targetType) >= getCharacterLength(sourceType));
        }
        if (targetType.is(DataTypeFamily.BINARY_STRING)
                && sourceType.is(DataTypeFamily.BINARY_STRING)) {
            return (targetType instanceof VarBinaryType
                            && getBinaryLength(targetType) >= getBinaryLength(sourceType))
                    || (targetType instanceof BinaryType
                            && sourceType instanceof BinaryType
                            && getBinaryLength(targetType) >= getBinaryLength(sourceType));
        }
        return isTemporalWithPrecision(targetType)
                && targetType.getClass().equals(sourceType.getClass())
                && getTemporalPrecision(targetType) >= getTemporalPrecision(sourceType);
    }

    private static Optional<DataType> getSafeWidenedType(DataType targetType, DataType sourceType) {
        boolean nullable = targetType.isNullable();
        targetType = targetType.notNull();
        sourceType = sourceType.notNull();

        if (targetType.is(DataTypeFamily.INTEGER_NUMERIC)
                && sourceType.is(DataTypeFamily.INTEGER_NUMERIC)) {
            return Optional.of(
                    (integerRank(targetType) >= integerRank(sourceType) ? targetType : sourceType)
                            .copy(nullable));
        }
        if (targetType.is(DataTypeFamily.APPROXIMATE_NUMERIC)
                && sourceType.is(DataTypeFamily.APPROXIMATE_NUMERIC)) {
            return Optional.of(
                    (approximateNumericRank(targetType) >= approximateNumericRank(sourceType)
                                    ? targetType
                                    : sourceType)
                            .copy(nullable));
        }
        if (targetType instanceof DecimalType && sourceType instanceof DecimalType) {
            DecimalType targetDecimal = (DecimalType) targetType;
            DecimalType sourceDecimal = (DecimalType) sourceType;
            int scale = Math.max(targetDecimal.getScale(), sourceDecimal.getScale());
            int integerDigits =
                    Math.max(
                            targetDecimal.getPrecision() - targetDecimal.getScale(),
                            sourceDecimal.getPrecision() - sourceDecimal.getScale());
            int precision = integerDigits + scale;
            if (precision <= DecimalType.MAX_PRECISION) {
                return Optional.of(new DecimalType(nullable, precision, scale));
            }
            return Optional.empty();
        }
        if (targetType.is(DataTypeFamily.CHARACTER_STRING)
                && sourceType.is(DataTypeFamily.CHARACTER_STRING)) {
            int length = Math.max(getCharacterLength(targetType), getCharacterLength(sourceType));
            if (targetType instanceof VarCharType || sourceType instanceof VarCharType) {
                return Optional.of(new VarCharType(nullable, length));
            }
            return Optional.of(new CharType(nullable, length));
        }
        if (targetType.is(DataTypeFamily.BINARY_STRING)
                && sourceType.is(DataTypeFamily.BINARY_STRING)) {
            int length = Math.max(getBinaryLength(targetType), getBinaryLength(sourceType));
            if (targetType instanceof VarBinaryType || sourceType instanceof VarBinaryType) {
                return Optional.of(new VarBinaryType(nullable, length));
            }
            return Optional.of(new BinaryType(nullable, length));
        }
        if (targetType.getClass().equals(sourceType.getClass())) {
            int precision =
                    Math.max(getTemporalPrecision(targetType), getTemporalPrecision(sourceType));
            if (targetType instanceof TimeType) {
                return Optional.of(new TimeType(nullable, precision));
            }
            if (targetType instanceof TimestampType) {
                return Optional.of(new TimestampType(nullable, precision));
            }
            if (targetType instanceof LocalZonedTimestampType) {
                return Optional.of(new LocalZonedTimestampType(nullable, precision));
            }
            if (targetType instanceof ZonedTimestampType) {
                return Optional.of(new ZonedTimestampType(nullable, precision));
            }
        }
        return Optional.empty();
    }

    private static int integerRank(DataType type) {
        DataTypeRoot root = type.getTypeRoot();
        switch (root) {
            case TINYINT:
                return 0;
            case SMALLINT:
                return 1;
            case INTEGER:
                return 2;
            case BIGINT:
                return 3;
            default:
                throw new IllegalArgumentException("Not an integer type: " + type);
        }
    }

    private static int approximateNumericRank(DataType type) {
        switch (type.getTypeRoot()) {
            case FLOAT:
                return 0;
            case DOUBLE:
                return 1;
            default:
                throw new IllegalArgumentException("Not an approximate numeric type: " + type);
        }
    }

    private static int getCharacterLength(DataType type) {
        if (type instanceof CharType) {
            return ((CharType) type).getLength();
        }
        return ((VarCharType) type).getLength();
    }

    private static int getBinaryLength(DataType type) {
        if (type instanceof BinaryType) {
            return ((BinaryType) type).getLength();
        }
        return ((VarBinaryType) type).getLength();
    }

    private static int getTemporalPrecision(DataType type) {
        if (type instanceof TimeType) {
            return ((TimeType) type).getPrecision();
        }
        if (type instanceof TimestampType) {
            return ((TimestampType) type).getPrecision();
        }
        if (type instanceof LocalZonedTimestampType) {
            return ((LocalZonedTimestampType) type).getPrecision();
        }
        if (type instanceof ZonedTimestampType) {
            return ((ZonedTimestampType) type).getPrecision();
        }
        return -1;
    }

    private static boolean isTemporalWithPrecision(DataType type) {
        return type instanceof TimeType
                || type instanceof TimestampType
                || type instanceof LocalZonedTimestampType
                || type instanceof ZonedTimestampType;
    }

    /** Outcome of a best-effort reconciliation attempt. */
    public enum ReconcileResult {
        NO_ACTION,
        REPAIRED,
        DELEGATE_TO_SINK
    }
}
