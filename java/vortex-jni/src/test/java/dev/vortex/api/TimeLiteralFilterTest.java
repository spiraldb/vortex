// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

package dev.vortex.api;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.vortex.api.Expression.BinaryOp;
import dev.vortex.api.Expression.TimeUnit;
import dev.vortex.arrow.ArrowAllocation;
import dev.vortex.jni.NativeLoader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end coverage for {@link Expression#literalTime(long, TimeUnit)} as a scan filter.
 *
 * <p>Time is an extension type over an {@code i32} or {@code i64} storage, so a predicate on a time column only works
 * if the literal carries the same unit as the column: comparing a seconds literal against a microseconds column is a
 * type error, not a silently rescaled comparison. These tests write a file with a {@code Time32(SECOND)} column and a
 * {@code Time64(MICROSECOND)} column and read it back through each predicate.
 *
 * <p>The file holds four rows at 00:00:00, 06:00:00, 12:00:00 and 18:00:00, tagged {@code 0..3} in an {@code id} column
 * so the surviving rows can be named without decoding the times again.
 */
public final class TimeLiteralFilterTest {
    private static final int ROW_COUNT = 4;
    private static final int SECONDS_PER_HOUR = 3600;
    private static final long MICROS_PER_SECOND = 1_000_000L;

    @TempDir
    static Path tempDir;

    private static Session session;
    private static String filePath;

    @BeforeAll
    public static void loadLibrary() {
        NativeLoader.loadJni();
    }

    @BeforeAll
    static void writeFile() throws IOException {
        session = Session.create();
        filePath = tempDir.resolve("time.vortex").toAbsolutePath().toUri().toString();

        BufferAllocator allocator = ArrowAllocation.rootAllocator();
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.notNullable("at_sec", new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.SECOND, 32)),
                Field.notNullable(
                        "at_micro", new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, 64))));

        try (VortexWriter writer = VortexWriter.builder(session, filePath, schema, allocator)
                        .build();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            IntVector id = (IntVector) root.getVector("id");
            TimeSecVector atSec = (TimeSecVector) root.getVector("at_sec");
            TimeMicroVector atMicro = (TimeMicroVector) root.getVector("at_micro");
            id.allocateNew(ROW_COUNT);
            atSec.allocateNew(ROW_COUNT);
            atMicro.allocateNew(ROW_COUNT);
            for (int i = 0; i < ROW_COUNT; i++) {
                id.setSafe(i, i);
                atSec.setSafe(i, hourSeconds(i * 6));
                atMicro.setSafe(i, hourSeconds(i * 6) * MICROS_PER_SECOND);
            }
            root.setRowCount(ROW_COUNT);

            try (ArrowArray array = ArrowArray.allocateNew(allocator);
                    ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator)) {
                Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
                writer.writeBatch(array.memoryAddress(), arrowSchema.memoryAddress());
            }
            writer.finish();
        }
    }

    @Test
    public void equalityOnASecondsTimeColumn() {
        assertEquals(
                List.of(2),
                scanIds(Expression.binary(
                        BinaryOp.EQ,
                        Expression.column("at_sec"),
                        Expression.literalTime(hourSeconds(12), TimeUnit.SECONDS))));
    }

    @Test
    public void anOrderingComparisonOnASecondsTimeColumn() {
        assertEquals(
                List.of(0, 1),
                scanIds(Expression.binary(
                        BinaryOp.LT,
                        Expression.column("at_sec"),
                        Expression.literalTime(hourSeconds(12), TimeUnit.SECONDS))));
    }

    @Test
    public void aBetweenOnAMicrosecondsTimeColumn() {
        Expression lower = Expression.literalTime(hourSeconds(6) * MICROS_PER_SECOND, TimeUnit.MICROSECONDS);
        Expression upper = Expression.literalTime(hourSeconds(12) * MICROS_PER_SECOND, TimeUnit.MICROSECONDS);
        assertEquals(
                List.of(1, 2),
                scanIds(Expression.between(
                        Expression.column("at_micro"),
                        lower,
                        upper,
                        /* lowerStrict= */ false,
                        /* upperStrict= */ false)));
    }

    @Test
    public void aNullTimeLiteralComparesToNothing() {
        // A comparison against a null literal is null for every row, so every row is filtered out.
        assertEquals(
                List.of(),
                scanIds(Expression.binary(
                        BinaryOp.EQ, Expression.column("at_sec"), Expression.nullLiteralTime(TimeUnit.SECONDS))));
    }

    private static int hourSeconds(int hour) {
        return hour * SECONDS_PER_HOUR;
    }

    /** Reads the {@code id} column of every row that survives {@code filter}, in file order. */
    private static List<Integer> scanIds(Expression filter) {
        BufferAllocator allocator = ArrowAllocation.rootAllocator();
        DataSource dataSource = DataSource.open(session, filePath);
        Scan scan = dataSource.scan(
                ScanOptions.builder().filter(filter).ordered(true).build());

        List<Integer> ids = new ArrayList<>();
        while (scan.hasNext()) {
            Partition partition = scan.next();
            try (ArrowReader reader = partition.scanArrow(allocator)) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot root = reader.getVectorSchemaRoot();
                    IntVector id = (IntVector) root.getVector("id");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        ids.add(id.get(i));
                    }
                }
            } catch (IOException e) {
                throw new AssertionError("failed reading partition", e);
            }
        }
        return ids;
    }
}
