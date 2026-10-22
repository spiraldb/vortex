// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

package dev.vortex.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests that selecting nested fields returns the requested field.
 *
 * <p>With {@code spark.sql.optimizer.nestedSchemaPruning.enabled} (the Spark default), Spark passes struct columns to
 * {@code pruneColumns} with unused fields removed. The native scan only prunes top-level columns, so the scan must
 * report the full struct type; otherwise Spark reads the pruned field ordinals out of the full struct and returns the
 * wrong field.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public final class VortexNestedSchemaPruningTest {

    private SparkSession spark;

    @TempDir
    Path tempDir;

    @BeforeAll
    public void setUp() {
        spark = SparkSession.builder()
                .appName("VortexNestedSchemaPruningTest")
                .master("local[2]")
                .config("spark.driver.host", "127.0.0.1")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.sql.adaptive.enabled", "false")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }

    @AfterAll
    public void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    private Dataset<Row> writeAndRead(String name) {
        Dataset<Row> df = spark.range(5)
                .select(
                        functions.col("id"),
                        functions
                                .struct(
                                        functions.col("id").plus(100).alias("a"),
                                        functions.col("id").plus(200).alias("b"),
                                        functions.col("id").plus(300).alias("c"))
                                .alias("s"));
        df = df.withColumn("arr", functions.array(functions.col("s")))
                .withColumn(
                        "outer",
                        functions.struct(
                                functions.lit("x").alias("p"),
                                functions.col("s").alias("inner")));
        String path = tempDir.resolve(name).toUri().toString();
        df.write()
                .format("vortex")
                .option("path", path)
                .mode(SaveMode.Overwrite)
                .save();
        return spark.read().format("vortex").option("path", path).load();
    }

    private static List<String> rows(Dataset<Row> df) {
        return df.orderBy("id").collectAsList().stream().map(Row::toString).collect(Collectors.toList());
    }

    @ParameterizedTest(name = "nestedSchemaPruning={0}")
    @ValueSource(strings = {"true", "false"})
    @DisplayName("Selecting a non-first struct field returns that field")
    public void selectNestedField(String nestedSchemaPruning) {
        spark.conf().set("spark.sql.optimizer.nestedSchemaPruning.enabled", nestedSchemaPruning);
        Dataset<Row> df = writeAndRead("struct_" + nestedSchemaPruning);

        assertEquals(List.of("[0,300]", "[1,301]", "[2,302]", "[3,303]", "[4,304]"), rows(df.select("id", "s.c")));
        assertEquals(
                List.of("[0,200,300]", "[1,201,301]", "[2,202,302]", "[3,203,303]", "[4,204,304]"),
                rows(df.select("id", "s.b", "s.c")));
        assertEquals(List.of("[3,303]", "[4,304]"), rows(df.filter("s.c >= 303").select("id", "s.c")));
    }

    @Test
    @DisplayName("Selecting a field of a struct inside an array or another struct returns that field")
    public void selectDeeplyNestedField() {
        spark.conf().set("spark.sql.optimizer.nestedSchemaPruning.enabled", "true");
        Dataset<Row> df = writeAndRead("deep");

        assertEquals(
                List.of("[0,300]", "[1,301]", "[2,302]", "[3,303]", "[4,304]"),
                rows(df.select(functions.col("id"), functions.element_at(functions.col("arr.c"), 1))));
        assertEquals(
                List.of("[0,300]", "[1,301]", "[2,302]", "[3,303]", "[4,304]"), rows(df.select("id", "outer.inner.c")));
    }
}
