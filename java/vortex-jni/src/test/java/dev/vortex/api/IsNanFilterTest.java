// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

package dev.vortex.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vortex.api.Expression.BinaryOp;
import dev.vortex.arrow.ArrowAllocation;
import dev.vortex.jni.NativeLoader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * End-to-end coverage for {@link Expression#isNan(Expression)} as a scan filter.
 *
 * <p>These tests exist to pin the behaviour that motivated the expression. Vortex compares floats with a total
 * ordering, so {@code f == f} is true for every non-null row — NaN included — and {@code f != f} is true for none:
 * {@link #equalityCannotIsolateNaN()} asserts exactly that, so the day float equality changes to IEEE semantics this
 * test says so rather than {@code isNan} quietly becoming redundant.
 *
 * <p>The file holds six rows tagged {@code 0..5} in an {@code id} column: two NaNs (one negative), an infinity, a zero,
 * an ordinary value, and a null.
 */
public final class IsNanFilterTest {
    private static final int NAN_ROW = 1;
    private static final int NEGATIVE_NAN_ROW = 3;
    private static final int NULL_ROW = 5;
    private static final Double[] VALUES = {
        1.5, Double.NaN, Double.POSITIVE_INFINITY, -Double.NaN, 0.0, null,
    };

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
        filePath = tempDir.resolve("is_nan.vortex").toAbsolutePath().toUri().toString();

        BufferAllocator allocator = ArrowAllocation.rootAllocator();
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                Field.nullable(
                        "value",
                        new ArrowType.FloatingPoint(org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE))));

        try (VortexWriter writer = VortexWriter.builder(session, filePath, schema, allocator)
                        .build();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            IntVector id = (IntVector) root.getVector("id");
            Float8Vector value = (Float8Vector) root.getVector("value");
            id.allocateNew(VALUES.length);
            value.allocateNew(VALUES.length);
            for (int i = 0; i < VALUES.length; i++) {
                id.setSafe(i, i);
                if (VALUES[i] == null) {
                    value.setNull(i);
                } else {
                    value.setSafe(i, VALUES[i]);
                }
            }
            root.setRowCount(VALUES.length);

            try (ArrowArray array = ArrowArray.allocateNew(allocator);
                    ArrowSchema arrowSchema = ArrowSchema.allocateNew(allocator)) {
                Data.exportVectorSchemaRoot(allocator, root, null, array, arrowSchema);
                writer.writeBatch(array.memoryAddress(), arrowSchema.memoryAddress());
            }
            writer.finish();
        }
    }

    @Test
    public void isNanMatchesEveryNaNAndNothingElse() {
        assertEquals(List.of(NAN_ROW, NEGATIVE_NAN_ROW), scanIds(Expression.isNan(Expression.column("value"))));
    }

    @Test
    public void isNotNanMatchesTheRestExceptNull() {
        // The null row survives neither predicate: isNan is strict, so NOT NaN inherits its null.
        assertEquals(List.of(0, 2, 4), scanIds(Expression.isNotNan(Expression.column("value"))));
    }

    @Test
    public void equalityCannotIsolateNaN() {
        // Why isNan has to exist. Under Vortex's total float ordering `value = value` keeps every non-null row,
        // NaN rows included, and `value != value` keeps none -- so neither can express an IS_NAN predicate.
        Expression column = Expression.column("value");
        assertEquals(List.of(0, 1, 2, 3, 4), scanIds(Expression.binary(BinaryOp.EQ, column, column)));
        assertEquals(List.of(), scanIds(Expression.binary(BinaryOp.NOT_EQ, column, column)));
        // A NaN literal does not help either: it matches only the NaN rows, but it also matches them under `=`,
        // which is not IEEE behaviour and is not something a translator can rely on.
        assertEquals(
                List.of(NAN_ROW, NEGATIVE_NAN_ROW),
                scanIds(Expression.binary(BinaryOp.EQ, column, Expression.literal(Double.NaN))));
    }

    @Test
    public void isNanOnANonFloatColumnIsATypeError() {
        // The dtype check happens when the filter is bound to the file's schema, so it surfaces from the scan
        // rather than from building the expression.
        RuntimeException exception =
                assertThrows(RuntimeException.class, () -> scanIds(Expression.isNan(Expression.column("id"))));
        assertTrue(
                exception.getMessage().contains("is_nan requires a float argument, got i32"),
                () -> "unexpected message: " + exception.getMessage());
    }

    @Test
    public void aNullRowIsNeitherNaNNorNotNaN() {
        assertEquals(List.of(NULL_ROW), scanIds(Expression.isNull(Expression.column("value"))));
        assertTrue(!scanIds(Expression.isNan(Expression.column("value"))).contains(NULL_ROW));
        assertTrue(!scanIds(Expression.isNotNan(Expression.column("value"))).contains(NULL_ROW));
    }

    /** Reads the {@code id} column of every row that survives {@code filter}, in file order. */
    private static List<Integer> scanIds(Expression filter) {
        BufferAllocator allocator = ArrowAllocation.rootAllocator();
        DataSource dataSource = DataSource.open(session, filePath);
        Scan scan = dataSource.scan(ScanOptions.builder()
                .filter(filter)
                .projection(Expression.select(new String[] {"id"}, Expression.root()))
                .ordered(true)
                .build());

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
                throw new UncheckedIOException("failed reading partition", e);
            }
        }
        return ids;
    }
}
