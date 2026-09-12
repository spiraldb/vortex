// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

package dev.vortex.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.vortex.api.Expression.BinaryOp;
import dev.vortex.api.Expression.DType;
import dev.vortex.arrow.ArrowAllocation;
import dev.vortex.jni.NativeLoader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Coverage for the literal types a predicate translator previously had to drop: geometry (and geography, which shares
 * the same Vortex extension type), Variant, and the type whose only value is null — Iceberg's {@code unknown}.
 *
 * <p>Geometry and unknown are exercised end to end, through a written file, because both are comparable: an extension
 * comparison goes through the storage but insists the two extension types match, so a geometry literal only filters a
 * column when its CRS agrees. Variant is not comparable — Vortex bails with "compare is not supported for dtype
 * variant" — so its literals are checked through the expression rendering instead. Building the literal is the part
 * that was missing; using one in a predicate additionally needs {@code variant_get} bound, which is not part of this
 * change.
 */
public final class ExtendedLiteralTest {
    private static final String EXTENSION_NAME_KEY = "ARROW:extension:name";
    private static final String EXTENSION_METADATA_KEY = "ARROW:extension:metadata";
    private static final String GEOARROW_WKB = "geoarrow.wkb";
    private static final String CRS = "OGC:CRS84";
    private static final String CRS_METADATA = "{\"crs\":\"" + CRS + "\"}";
    private static final int ROW_COUNT = 3;

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
        filePath = tempDir.resolve("extended_literals.vortex")
                .toAbsolutePath()
                .toUri()
                .toString();

        BufferAllocator allocator = ArrowAllocation.rootAllocator();
        Schema schema = new Schema(List.of(
                Field.notNullable("id", new ArrowType.Int(32, true)),
                new Field(
                        "geom",
                        new FieldType(
                                true,
                                ArrowType.Binary.INSTANCE,
                                null,
                                Map.of(EXTENSION_NAME_KEY, GEOARROW_WKB, EXTENSION_METADATA_KEY, CRS_METADATA)),
                        null),
                Field.nullable("unknown", ArrowType.Null.INSTANCE)));

        try (VortexWriter writer = VortexWriter.builder(session, filePath, schema, allocator)
                        .build();
                VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
            IntVector id = (IntVector) root.getVector("id");
            VarBinaryVector geom = (VarBinaryVector) root.getVector("geom");
            id.allocateNew(ROW_COUNT);
            geom.allocateNew(ROW_COUNT);
            for (int i = 0; i < ROW_COUNT; i++) {
                id.setSafe(i, i);
                geom.setSafe(i, wkbPoint(i, i));
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
    public void aGeometryLiteralFiltersAWkbColumn() {
        Expression literal = Expression.literalGeometry(wkbPoint(1, 1), CRS);
        assertEquals(List.of(1), scanIds(Expression.binary(BinaryOp.EQ, Expression.column("geom"), literal)));
    }

    @Test
    public void aGeometryLiteralWithADifferentCrsIsNotComparable() {
        // Extension comparison requires the two extension dtypes to agree, and the CRS is part of the dtype. A CRS
        // mismatch has to fail loudly rather than silently comparing raw WKB bytes across coordinate systems.
        Expression literal = Expression.literalGeometry(wkbPoint(1, 1), "EPSG:4326");
        UncheckedIOException exception = assertThrows(
                UncheckedIOException.class,
                () -> scanIds(Expression.binary(BinaryOp.EQ, Expression.column("geom"), literal)));
        String message = exception.getCause().getMessage();
        assertTrue(
                message.contains("Cannot compare scalars with incompatible types")
                        && message.contains("crs=OGC:CRS84")
                        && message.contains("crs=EPSG:4326"),
                () -> "unexpected message: " + message);
    }

    @Test
    public void aGeometryLiteralRejectsBytesThatAreNotWkb() {
        RuntimeException exception =
                assertThrows(RuntimeException.class, () -> Expression.literalGeometry(new byte[] {0x7f}, CRS));
        assertTrue(
                exception.getMessage().contains("failed parsing WKB"),
                () -> "unexpected message: " + exception.getMessage());
    }

    @Test
    public void aNullGeometryLiteralMatchesNothing() {
        Expression literal = Expression.nullLiteralGeometry(CRS);
        assertEquals(List.of(), scanIds(Expression.binary(BinaryOp.EQ, Expression.column("geom"), literal)));
    }

    @Test
    public void anUnknownColumnComparesToTheNullDType() {
        // Iceberg's `unknown` is a column of nothing but nulls. Every comparison against it is null, so the predicate
        // keeps no rows -- but it type-checks, which is what lets a translator push it down instead of dropping it.
        assertEquals(
                List.of(),
                scanIds(Expression.binary(
                        BinaryOp.EQ, Expression.column("unknown"), Expression.nullLiteral(DType.NULL))));
        assertEquals(List.of(0, 1, 2), scanIds(Expression.isNull(Expression.column("unknown"))));
    }

    @Test
    public void aVariantLiteralWrapsTheValueItHolds() {
        assertEquals(
                "variant(42i64)",
                Expression.literalVariant(Expression.literal(42L)).toString());
        assertEquals(
                "variant(\"hello\")",
                Expression.literalVariant(Expression.literal("hello")).toString());
    }

    @Test
    public void aVariantHoldingNullIsNotANullVariant() {
        // The distinction Variant requires: an absent variant, versus a present variant whose value is null.
        assertEquals("null", Expression.nullLiteral(DType.VARIANT).toString());
        assertEquals(
                "variant(null)",
                Expression.literalVariant(Expression.nullLiteral(DType.NULL)).toString());
    }

    @Test
    public void aVariantLiteralMustWrapALiteral() {
        RuntimeException exception =
                assertThrows(RuntimeException.class, () -> Expression.literalVariant(Expression.column("v")));
        assertTrue(
                exception.getMessage().contains("variant literal must wrap a literal"),
                () -> "unexpected message: " + exception.getMessage());
    }

    @Test
    public void theNewNullLiteralDTypesBuild() {
        assertNotNull(Expression.nullLiteral(DType.NULL));
        assertNotNull(Expression.nullLiteral(DType.VARIANT));
    }

    /** Little-endian WKB encoding of {@code POINT(x y)}. */
    private static byte[] wkbPoint(double x, double y) {
        ByteBuffer buffer = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1); // little-endian marker
        buffer.putInt(1); // geometry type: point
        buffer.putDouble(x);
        buffer.putDouble(y);
        return buffer.array();
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
