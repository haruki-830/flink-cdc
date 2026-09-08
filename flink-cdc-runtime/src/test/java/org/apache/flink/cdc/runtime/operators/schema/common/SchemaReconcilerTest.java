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

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.AlterColumnTypeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.exceptions.SchemaEvolveException;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.sink.SchemaAwareMetadataApplier;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.types.TimestampType;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.runtime.operators.schema.common.SchemaReconciler.ReconcileResult;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Tests for {@link SchemaReconciler}. */
class SchemaReconcilerTest {

    private static final TableId TABLE_ID = TableId.tableId("inventory", "products");

    @Test
    void testDoesNothingWhenTargetTableDoesNotExist() {
        TestingSchemaAwareMetadataApplier applier = new TestingSchemaAwareMetadataApplier(null);

        assertThat(
                        new SchemaReconciler(applier, SchemaChangeBehavior.LENIENT)
                                .reconcile(eventWithMissingColumn()))
                .isEqualTo(ReconcileResult.NO_ACTION);
        assertThat(applier.queryCalls).isOne();
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testAddsMissingColumnsAsNullableIdempotently() {
        TestingSchemaAwareMetadataApplier applier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        SchemaReconciler reconciler = new SchemaReconciler(applier, SchemaChangeBehavior.LENIENT);
        CreateTableEvent event =
                createTableEvent(
                        Column.physicalColumn("id", DataTypes.INT()),
                        Column.physicalColumn("name", DataTypes.STRING().notNull()));

        assertThat(reconciler.reconcile(event)).isEqualTo(ReconcileResult.REPAIRED);
        assertThat(reconciler.reconcile(event)).isEqualTo(ReconcileResult.NO_ACTION);

        assertThat(applier.appliedEvents).hasSize(1);
        AddColumnEvent addColumnEvent = (AddColumnEvent) applier.appliedEvents.get(0);
        assertThat(addColumnEvent.getAddedColumns())
                .extracting(AddColumnEvent.ColumnWithPosition::getAddColumn)
                .containsExactly(Column.physicalColumn("name", DataTypes.STRING()));
    }

    @Test
    void testDoesNotAddMissingPrimaryOrPartitionKeys() {
        TestingSchemaAwareMetadataApplier applier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("value", DataTypes.STRING())));
        Schema pipelineSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .physicalColumn("region", DataTypes.STRING().notNull())
                        .physicalColumn("value", DataTypes.STRING())
                        .primaryKey("id")
                        .partitionKey("region")
                        .build();

        assertThat(
                        new SchemaReconciler(applier, SchemaChangeBehavior.LENIENT)
                                .reconcile(new CreateTableEvent(TABLE_ID, pipelineSchema)))
                .isEqualTo(ReconcileResult.DELEGATE_TO_SINK);
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testDoesNothingWhenTargetTypeIsWider() {
        TestingSchemaAwareMetadataApplier applier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("value", DataTypes.BIGINT())));

        assertThat(
                        new SchemaReconciler(applier, SchemaChangeBehavior.LENIENT)
                                .reconcile(
                                        createTableEvent(
                                                Column.physicalColumn("value", DataTypes.INT()))))
                .isEqualTo(ReconcileResult.NO_ACTION);
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testWidensNarrowTargetTypeIdempotently() {
        TestingSchemaAwareMetadataApplier applier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("value", DataTypes.INT())));
        SchemaReconciler reconciler = new SchemaReconciler(applier, SchemaChangeBehavior.LENIENT);
        CreateTableEvent event =
                createTableEvent(Column.physicalColumn("value", DataTypes.BIGINT()));

        assertThat(reconciler.reconcile(event)).isEqualTo(ReconcileResult.REPAIRED);
        assertThat(reconciler.reconcile(event)).isEqualTo(ReconcileResult.NO_ACTION);

        assertThat(applier.appliedEvents).singleElement().isInstanceOf(AlterColumnTypeEvent.class);
        AlterColumnTypeEvent alterColumnTypeEvent =
                (AlterColumnTypeEvent) applier.appliedEvents.get(0);
        assertThat(alterColumnTypeEvent.getTypeMapping())
                .containsExactly(Map.entry("value", DataTypes.BIGINT()));
        assertThat(alterColumnTypeEvent.getOldTypeMapping())
                .containsExactly(Map.entry("value", DataTypes.INT()));
    }

    @Test
    void testDoesNotWidenKeyColumns() {
        Schema targetSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.INT().notNull())
                        .primaryKey("id")
                        .build();
        Schema pipelineSchema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull())
                        .primaryKey("id")
                        .build();
        TestingSchemaAwareMetadataApplier applier =
                new TestingSchemaAwareMetadataApplier(targetSchema);

        assertThat(
                        new SchemaReconciler(applier, SchemaChangeBehavior.LENIENT)
                                .reconcile(new CreateTableEvent(TABLE_ID, pipelineSchema)))
                .isEqualTo(ReconcileResult.DELEGATE_TO_SINK);
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testWidensDecimalWithoutReducingExistingIntegerRange() {
        TestingSchemaAwareMetadataApplier applier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("amount", DataTypes.DECIMAL(12, 2))));

        assertThat(
                        new SchemaReconciler(applier, SchemaChangeBehavior.LENIENT)
                                .reconcile(
                                        createTableEvent(
                                                Column.physicalColumn(
                                                        "amount", DataTypes.DECIMAL(12, 4)))))
                .isEqualTo(ReconcileResult.REPAIRED);

        AlterColumnTypeEvent event = (AlterColumnTypeEvent) applier.appliedEvents.get(0);
        assertThat(event.getTypeMapping())
                .containsExactly(Map.entry("amount", DataTypes.DECIMAL(14, 4)));
    }

    @ParameterizedTest
    @MethodSource("safeWideningCases")
    void testSafeWideningFamilies(
            DataType targetType, DataType pipelineType, DataType expectedWidenedType) {
        TestingSchemaAwareMetadataApplier applier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("value", targetType)));

        assertThat(
                        new SchemaReconciler(applier, SchemaChangeBehavior.LENIENT)
                                .reconcile(
                                        createTableEvent(
                                                Column.physicalColumn("value", pipelineType))))
                .isEqualTo(ReconcileResult.REPAIRED);

        AlterColumnTypeEvent event = (AlterColumnTypeEvent) applier.appliedEvents.get(0);
        assertThat(event.getTypeMapping()).containsExactly(Map.entry("value", expectedWidenedType));
    }

    @ParameterizedTest
    @MethodSource("unsafeWideningCases")
    void testDelegatesTypesWithoutConservativeWidening(DataType targetType, DataType pipelineType) {
        TestingSchemaAwareMetadataApplier applier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("value", targetType)));

        assertThat(
                        new SchemaReconciler(applier, SchemaChangeBehavior.LENIENT)
                                .reconcile(
                                        createTableEvent(
                                                Column.physicalColumn("value", pipelineType))))
                .isEqualTo(ReconcileResult.DELEGATE_TO_SINK);
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testDelegatesCompletelyIncompatibleTypes() {
        TestingSchemaAwareMetadataApplier applier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("value", DataTypes.BOOLEAN())));

        assertThat(
                        new SchemaReconciler(applier, SchemaChangeBehavior.LENIENT)
                                .reconcile(
                                        createTableEvent(
                                                Column.physicalColumn(
                                                        "value", DataTypes.TIMESTAMP()))))
                .isEqualTo(ReconcileResult.DELEGATE_TO_SINK);
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testNormalizationAvoidsUnimplementableAlter() {
        TestingSchemaAwareMetadataApplier applier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("event_time", DataTypes.TIMESTAMP(6))));
        applier.normalizer =
                type -> {
                    if (type instanceof TimestampType) {
                        return DataTypes.TIMESTAMP(
                                        Math.min(((TimestampType) type).getPrecision(), 6))
                                .copy(type.isNullable());
                    }
                    return type;
                };

        assertThat(
                        new SchemaReconciler(applier, SchemaChangeBehavior.LENIENT)
                                .reconcile(
                                        createTableEvent(
                                                Column.physicalColumn(
                                                        "event_time", DataTypes.TIMESTAMP(9)))))
                .isEqualTo(ReconcileResult.NO_ACTION);
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testDelegatesNullableToNotNullDifference() {
        TestingSchemaAwareMetadataApplier applier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("value", DataTypes.BIGINT().notNull())));

        assertThat(
                        new SchemaReconciler(applier, SchemaChangeBehavior.LENIENT)
                                .reconcile(
                                        createTableEvent(
                                                Column.physicalColumn(
                                                        "value", DataTypes.BIGINT()))))
                .isEqualTo(ReconcileResult.DELEGATE_TO_SINK);
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testDelegatesWhenRepairTypeIsUnavailable() {
        TestingSchemaAwareMetadataApplier addUnavailableApplier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        addUnavailableApplier.acceptedEventTypes =
                Collections.singleton(SchemaChangeEventType.CREATE_TABLE);

        assertThat(
                        new SchemaReconciler(addUnavailableApplier, SchemaChangeBehavior.LENIENT)
                                .reconcile(eventWithMissingColumn()))
                .isEqualTo(ReconcileResult.DELEGATE_TO_SINK);
        assertThat(addUnavailableApplier.appliedEvents).isEmpty();

        TestingSchemaAwareMetadataApplier alterUnavailableApplier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        alterUnavailableApplier.supportedEventTypes =
                Collections.singleton(SchemaChangeEventType.ADD_COLUMN);

        assertThat(
                        new SchemaReconciler(alterUnavailableApplier, SchemaChangeBehavior.LENIENT)
                                .reconcile(
                                        createTableEvent(
                                                Column.physicalColumn("id", DataTypes.BIGINT()))))
                .isEqualTo(ReconcileResult.DELEGATE_TO_SINK);
        assertThat(alterUnavailableApplier.appliedEvents).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(
            value = SchemaChangeBehavior.class,
            names = {"IGNORE", "EXCEPTION"})
    void testDoesNotRepairWhenSchemaChangeBehaviorDisablesEvolution(
            SchemaChangeBehavior schemaChangeBehavior) {
        TestingSchemaAwareMetadataApplier applier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        CreateTableEvent event =
                createTableEvent(
                        Column.physicalColumn("id", DataTypes.BIGINT()),
                        Column.physicalColumn("name", DataTypes.STRING()));

        assertThat(new SchemaReconciler(applier, schemaChangeBehavior).reconcile(event))
                .isEqualTo(ReconcileResult.DELEGATE_TO_SINK);
        assertThat(applier.appliedEvents).isEmpty();
    }

    @Test
    void testNeverFailsFastOnQueryNormalizationOrDdlFailure() {
        TestingSchemaAwareMetadataApplier queryFailureApplier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        queryFailureApplier.queryFailureAfterCalls = 0;
        SchemaReconciler queryFailureReconciler =
                new SchemaReconciler(queryFailureApplier, SchemaChangeBehavior.LENIENT);
        assertThatCode(() -> queryFailureReconciler.reconcile(eventWithMissingColumn()))
                .doesNotThrowAnyException();
        assertThat(queryFailureReconciler.reconcile(eventWithMissingColumn()))
                .isEqualTo(ReconcileResult.DELEGATE_TO_SINK);

        TestingSchemaAwareMetadataApplier normalizationFailureApplier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        normalizationFailureApplier.normalizer =
                type -> {
                    throw new RuntimeException("Expected normalization failure");
                };
        assertThat(
                        new SchemaReconciler(
                                        normalizationFailureApplier, SchemaChangeBehavior.LENIENT)
                                .reconcile(
                                        createTableEvent(
                                                Column.physicalColumn("id", DataTypes.BIGINT()))))
                .isEqualTo(ReconcileResult.DELEGATE_TO_SINK);

        TestingSchemaAwareMetadataApplier ddlFailureApplier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        ddlFailureApplier.failedEventTypes.add(SchemaChangeEventType.ADD_COLUMN);
        assertThat(
                        new SchemaReconciler(ddlFailureApplier, SchemaChangeBehavior.LENIENT)
                                .reconcile(eventWithMissingColumn()))
                .isEqualTo(ReconcileResult.DELEGATE_TO_SINK);
    }

    @Test
    void testDelegatesWhenRepairCannotBeConfirmed() {
        TestingSchemaAwareMetadataApplier unchangedTargetApplier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        unchangedTargetApplier.updateTargetSchema = false;

        assertThat(
                        new SchemaReconciler(unchangedTargetApplier, SchemaChangeBehavior.LENIENT)
                                .reconcile(eventWithMissingColumn()))
                .isEqualTo(ReconcileResult.DELEGATE_TO_SINK);
        assertThat(unchangedTargetApplier.appliedEvents)
                .singleElement()
                .isInstanceOf(AddColumnEvent.class);

        TestingSchemaAwareMetadataApplier unchangedTargetTypeApplier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        unchangedTargetTypeApplier.updateTargetSchema = false;
        assertThat(
                        new SchemaReconciler(
                                        unchangedTargetTypeApplier, SchemaChangeBehavior.LENIENT)
                                .reconcile(
                                        createTableEvent(
                                                Column.physicalColumn("id", DataTypes.BIGINT()))))
                .isEqualTo(ReconcileResult.DELEGATE_TO_SINK);
        assertThat(unchangedTargetTypeApplier.appliedEvents)
                .singleElement()
                .isInstanceOf(AlterColumnTypeEvent.class);

        TestingSchemaAwareMetadataApplier readBackFailureApplier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("id", DataTypes.INT())));
        readBackFailureApplier.queryFailureAfterCalls = 1;
        assertThat(
                        new SchemaReconciler(readBackFailureApplier, SchemaChangeBehavior.LENIENT)
                                .reconcile(eventWithMissingColumn()))
                .isEqualTo(ReconcileResult.DELEGATE_TO_SINK);
    }

    @Test
    void testAppliesAddAndAlterInOneReconciliation() {
        TestingSchemaAwareMetadataApplier applier =
                new TestingSchemaAwareMetadataApplier(
                        schema(Column.physicalColumn("id", DataTypes.INT())));

        assertThat(
                        new SchemaReconciler(applier, SchemaChangeBehavior.LENIENT)
                                .reconcile(
                                        createTableEvent(
                                                Column.physicalColumn("id", DataTypes.BIGINT()),
                                                Column.physicalColumn(
                                                        "description",
                                                        DataTypes.STRING().notNull()))))
                .isEqualTo(ReconcileResult.REPAIRED);
        assertThat(applier.appliedEvents)
                .extracting(SchemaChangeEvent::getType)
                .containsExactly(
                        SchemaChangeEventType.ADD_COLUMN, SchemaChangeEventType.ALTER_COLUMN_TYPE);
    }

    private static CreateTableEvent eventWithMissingColumn() {
        return createTableEvent(
                Column.physicalColumn("id", DataTypes.INT()),
                Column.physicalColumn("name", DataTypes.STRING()));
    }

    private static CreateTableEvent createTableEvent(Column... columns) {
        return new CreateTableEvent(TABLE_ID, schema(columns));
    }

    private static Schema schema(Column... columns) {
        return Schema.newBuilder().setColumns(Arrays.asList(columns)).build();
    }

    private static Stream<Arguments> safeWideningCases() {
        return Stream.of(
                Arguments.arguments(
                        DataTypes.TINYINT(), DataTypes.SMALLINT(), DataTypes.SMALLINT()),
                Arguments.arguments(DataTypes.SMALLINT(), DataTypes.INT(), DataTypes.INT()),
                Arguments.arguments(DataTypes.FLOAT(), DataTypes.DOUBLE(), DataTypes.DOUBLE()),
                Arguments.arguments(DataTypes.CHAR(4), DataTypes.CHAR(8), DataTypes.CHAR(8)),
                Arguments.arguments(DataTypes.CHAR(8), DataTypes.VARCHAR(4), DataTypes.VARCHAR(8)),
                Arguments.arguments(
                        DataTypes.VARCHAR(4), DataTypes.VARCHAR(8), DataTypes.VARCHAR(8)),
                Arguments.arguments(DataTypes.BINARY(4), DataTypes.BINARY(8), DataTypes.BINARY(8)),
                Arguments.arguments(
                        DataTypes.BINARY(8), DataTypes.VARBINARY(4), DataTypes.VARBINARY(8)),
                Arguments.arguments(
                        DataTypes.VARBINARY(4), DataTypes.VARBINARY(8), DataTypes.VARBINARY(8)),
                Arguments.arguments(DataTypes.TIME(3), DataTypes.TIME(6), DataTypes.TIME(6)),
                Arguments.arguments(
                        DataTypes.TIMESTAMP(3), DataTypes.TIMESTAMP(6), DataTypes.TIMESTAMP(6)),
                Arguments.arguments(
                        DataTypes.TIMESTAMP_LTZ(3),
                        DataTypes.TIMESTAMP_LTZ(6),
                        DataTypes.TIMESTAMP_LTZ(6)),
                Arguments.arguments(
                        DataTypes.TIMESTAMP_TZ(3),
                        DataTypes.TIMESTAMP_TZ(6),
                        DataTypes.TIMESTAMP_TZ(6)));
    }

    private static Stream<Arguments> unsafeWideningCases() {
        return Stream.of(
                Arguments.arguments(DataTypes.BIGINT(), DataTypes.FLOAT()),
                Arguments.arguments(DataTypes.DECIMAL(38, 0), DataTypes.DECIMAL(38, 38)),
                Arguments.arguments(DataTypes.TIMESTAMP(6), DataTypes.TIMESTAMP_LTZ(6)),
                Arguments.arguments(DataTypes.VARCHAR(8), DataTypes.VARBINARY(8)));
    }

    private static class TestingSchemaAwareMetadataApplier implements SchemaAwareMetadataApplier {

        private Schema targetSchema;
        private Set<SchemaChangeEventType> acceptedEventTypes =
                EnumSet.allOf(SchemaChangeEventType.class);
        private Set<SchemaChangeEventType> supportedEventTypes =
                EnumSet.allOf(SchemaChangeEventType.class);
        private final Set<SchemaChangeEventType> failedEventTypes =
                EnumSet.noneOf(SchemaChangeEventType.class);
        private final List<SchemaChangeEvent> appliedEvents = new ArrayList<>();
        private boolean updateTargetSchema = true;
        private int queryFailureAfterCalls = Integer.MAX_VALUE;
        private int queryCalls;
        private UnaryOperator<DataType> normalizer = type -> type;

        private TestingSchemaAwareMetadataApplier(Schema targetSchema) {
            this.targetSchema = targetSchema;
        }

        @Override
        public Optional<Schema> getTargetTableSchema(TableId tableId) {
            if (queryCalls++ >= queryFailureAfterCalls) {
                throw new RuntimeException("Expected target schema query failure");
            }
            return Optional.ofNullable(targetSchema);
        }

        @Override
        public DataType normalizeToTargetDataType(
                TableId tableId, String columnName, DataType pipelineDataType) {
            return normalizer.apply(pipelineDataType);
        }

        @Override
        public void applySchemaChange(SchemaChangeEvent schemaChangeEvent)
                throws SchemaEvolveException {
            if (failedEventTypes.contains(schemaChangeEvent.getType())) {
                throw new SchemaEvolveException(schemaChangeEvent, "Expected DDL failure");
            }
            appliedEvents.add(schemaChangeEvent);
            if (updateTargetSchema) {
                targetSchema = SchemaUtils.applySchemaChangeEvent(targetSchema, schemaChangeEvent);
            }
        }

        @Override
        public boolean acceptsSchemaEvolutionType(SchemaChangeEventType schemaChangeEventType) {
            return acceptedEventTypes.contains(schemaChangeEventType);
        }

        @Override
        public Set<SchemaChangeEventType> getSupportedSchemaEvolutionTypes() {
            return supportedEventTypes;
        }
    }
}
