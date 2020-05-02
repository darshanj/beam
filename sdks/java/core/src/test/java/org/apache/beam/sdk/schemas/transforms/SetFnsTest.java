/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.schemas.transforms;

import static junit.framework.TestCase.assertEquals;

import java.math.BigDecimal;
import java.util.List;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for SetFns transform. */
@RunWith(JUnit4.class)
public class SetFnsTest {
  @Rule public final TestPipeline p = TestPipeline.create();

  @Test
  @Category(ValidatesRunner.class)
  public void testIntersection() {

    PCollection<String> left =
        p.apply(
            "left",
            Create.of("a", "a", "b", "b", "c", "c", "d", "d").withCoder(StringUtf8Coder.of()));

    PCollection<String> right =
        p.apply(
            "right",
            Create.of("a", "a", "b", "b", "e", "e", "f", "f").withCoder(StringUtf8Coder.of()));

    PCollection<String> results = left.apply(SetFns.intersect(right));
    PAssert.that(results).containsInAnyOrder("a", "b");

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testIntersectionAll() {

    PCollection<String> left =
        p.apply(
            "left",
            Create.of("a", "a", "a", "b", "b", "c", "d", "d").withCoder(StringUtf8Coder.of()));

    PCollection<String> right =
        p.apply(
            "right",
            Create.of("a", "a", "b", "b", "b", "c", "e", "e", "f", "f")
                .withCoder(StringUtf8Coder.of()));

    PCollection<String> results = left.apply(SetFns.intersectAll(right));
    PAssert.that(results).containsInAnyOrder("a", "a", "b", "b", "c");

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testExcept() {

    PCollection<String> left =
        p.apply(
            "left",
            Create.of("a", "a", "b", "b", "c", "c", "d", "d").withCoder(StringUtf8Coder.of()));

    PCollection<String> right =
        p.apply(
            "right",
            Create.of("a", "a", "b", "b", "e", "e", "f", "f").withCoder(StringUtf8Coder.of()));

    PCollection<String> results = left.apply(SetFns.except(right));
    PAssert.that(results).containsInAnyOrder("c", "d");

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testExceptAll() {

    // Say for Row R, there are m instances on left and n instances on right:
    // - EXCEPT ALL outputs MAX(m - n, 0) instances of R.
    // - EXCEPT [DISTINCT] outputs a single instance of R if m > 0 and n == 0, else
    //   they output 0 instances.

    PCollection<String> left =
        p.apply("left", Create.of("1", "1", "2", "4", "4").withCoder(StringUtf8Coder.of()));

    PCollection<String> right =
        p.apply("right", Create.of("1", "2", "3").withCoder(StringUtf8Coder.of()));

    PCollection<String> results = left.apply(SetFns.exceptAll(right));
    PAssert.that(results).containsInAnyOrder("1", "4", "4");

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testExceptRow() {

    Schema schema =
        Schema.builder()
            .addInt64Field("order_id")
            .addInt32Field("site_id")
            .addDecimalField("price")
            .build();
    PCollection<Row> left =
        p.apply(
            "left",
            Create.of(
                    Row.withSchema(schema).addValues(1L, 1, new BigDecimal(1.0)).build(),
                    Row.withSchema(schema).addValues(1L, 1, new BigDecimal(1.0)).build(),
                    Row.withSchema(schema).addValues(2L, 2, new BigDecimal(2.0)).build(),
                    Row.withSchema(schema).addValues(4L, 4, new BigDecimal(4.0)).build(),
                    Row.withSchema(schema).addValues(4L, 4, new BigDecimal(4.0)).build())
                .withRowSchema(schema));

    PCollection<Row> right =
        p.apply(
            "right",
            Create.of(
                    Row.withSchema(schema).addValues(1L, 1, new BigDecimal(1.0)).build(),
                    Row.withSchema(schema).addValues(2L, 2, new BigDecimal(2.0)).build(),
                    Row.withSchema(schema).addValues(3L, 3, new BigDecimal(3.0)).build())
                .withRowSchema(schema));

    PCollection<Row> results = left.apply(SetFns.except(right));

    assertEquals(schema, results.getSchema());

    List<Row> expectedRows =
        ImmutableList.of(Row.withSchema(schema).addValues(4L, 4, new BigDecimal(4.0)).build());
    PAssert.that(results).containsInAnyOrder(expectedRows);
    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testExceptRowAll() {

    Schema schema =
        Schema.builder()
            .addInt64Field("order_id")
            .addInt32Field("site_id")
            .addDecimalField("price")
            .build();
    PCollection<Row> left =
        p.apply(
            "left",
            Create.of(
                    Row.withSchema(schema).addValues(1L, 1, new BigDecimal(1.0)).build(),
                    Row.withSchema(schema).addValues(1L, 1, new BigDecimal(1.0)).build(),
                    Row.withSchema(schema).addValues(2L, 2, new BigDecimal(2.0)).build(),
                    Row.withSchema(schema).addValues(4L, 4, new BigDecimal(4.0)).build(),
                    Row.withSchema(schema).addValues(4L, 4, new BigDecimal(4.0)).build())
                .withRowSchema(schema));

    PCollection<Row> right =
        p.apply(
            "right",
            Create.of(
                    Row.withSchema(schema).addValues(1L, 1, new BigDecimal(1.0)).build(),
                    Row.withSchema(schema).addValues(2L, 2, new BigDecimal(2.0)).build(),
                    Row.withSchema(schema).addValues(3L, 3, new BigDecimal(3.0)).build())
                .withRowSchema(schema));

    PCollection<Row> results = left.apply(SetFns.except(right));

    assertEquals(schema, results.getSchema());

    List<Row> expectedRows =
        ImmutableList.of(Row.withSchema(schema).addValues(4L, 4, new BigDecimal(4.0)).build());
    PAssert.that(results).containsInAnyOrder(expectedRows);
    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testUnion() {

    PCollection<String> left =
        p.apply(
            "left",
            Create.of("a", "a", "b", "b", "c", "c", "d", "d").withCoder(StringUtf8Coder.of()));

    PCollection<String> right =
        p.apply(
            "right",
            Create.of("a", "a", "b", "b", "e", "e", "f", "f").withCoder(StringUtf8Coder.of()));

    PCollection<String> results = left.apply(SetFns.union(right));
    PAssert.that(results).containsInAnyOrder("a", "b", "c", "d", "e", "f");

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testUnionAll() {

    PCollection<String> left =
        p.apply(
            "left",
            Create.of("a", "a", "b", "b", "c", "c", "d", "d").withCoder(StringUtf8Coder.of()));

    PCollection<String> right =
        p.apply(
            "right",
            Create.of("a", "a", "b", "b", "e", "e", "f", "f").withCoder(StringUtf8Coder.of()));

    PCollection<String> results = left.apply(SetFns.unionAll(right));
    PAssert.that(results)
        .containsInAnyOrder(
            "a", "a", "a", "a", "b", "b", "b", "b", "c", "c", "d", "d", "e", "e", "f", "f");

    p.run();
  }
}
