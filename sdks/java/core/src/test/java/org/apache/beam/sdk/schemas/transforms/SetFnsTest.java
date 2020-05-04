package org.apache.beam.sdk.schemas.transforms;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;

import static junit.framework.TestCase.assertEquals;

@RunWith(JUnit4.class)
public class SetFnsTest {

  @Rule
  public final TestPipeline p = TestPipeline.create();

  Schema schema = Schema.builder().addStringField("alphabet").build();

  static PCollection<String> left;
  static PCollection<String> right;
  static PCollection<Row> leftRow;
  static PCollection<Row> rightRow;

  private Iterable<Row> toRows(String... values) {
    return Iterables.transform(Arrays.asList(values), (elem) -> Row.withSchema(schema).addValues(elem).build());
  }

  @Before
  public void setup() {

    String[] leftData = {"a", "a", "a", "b", "b", "c", "d", "d", "g", "g", "h", "h"};
    String[] rightData = {"a", "a", "b", "b", "b", "c", "d", "d", "e", "e", "f", "f"};

    left = p.apply("left", Create.of(Arrays.asList(leftData)));
    right = p.apply("right", Create.of(Arrays.asList(rightData)));
    leftRow = p.apply("leftRow", Create.of(toRows(leftData)).withRowSchema(schema));
    rightRow = p.apply("rightRow", Create.of(toRows(rightData)).withRowSchema(schema));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testIntersection() {
    PAssert.that(left.apply("stringleft",SetFns.intersect(right))).containsInAnyOrder("a", "b", "c", "d");

    PCollection<Row> results = leftRow.apply(SetFns.intersect(rightRow));
    PAssert.that(results).containsInAnyOrder(toRows("a", "b", "c", "d"));
    assertEquals(schema, results.getSchema());

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testIntersectionAll() {
    // Say for Row R, there are m instances on left and n instances on right,
    // INTERSECT ALL outputs MIN(m, n) instances of R.

    PAssert.that(left.apply("strings",SetFns.intersectAll(right))).containsInAnyOrder("a", "a", "b", "b", "c", "d", "d");
    PAssert.that(leftRow.apply("rows",SetFns.intersectAll(rightRow))).containsInAnyOrder(toRows("a", "a", "b", "b", "c", "d", "d"));

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testExcept() {

    PAssert.that(left.apply("strings",SetFns.except(right))).containsInAnyOrder("g", "h");
    PAssert.that(leftRow.apply("rows",SetFns.except(rightRow))).containsInAnyOrder(toRows("g", "h"));
    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testExceptAll() {

    // Say for Row R, there are m instances on left and n instances on right:
    // - EXCEPT ALL outputs MAX(m - n, 0) instances of R.
    // - EXCEPT [DISTINCT] outputs a single instance of R if m > 0 and n == 0, else
    //   they output 0 instances.

    PAssert.that(left.apply("strings",SetFns.exceptAll(right))).containsInAnyOrder("a", "g", "g", "h", "h");
    PAssert.that(leftRow.apply("rows",SetFns.exceptAll(rightRow))).containsInAnyOrder(toRows("a", "g", "g", "h", "h"));

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testUnion() {

    PAssert.that(left.apply("strings",SetFns.union(right))).containsInAnyOrder("a", "b", "c", "d", "e", "f", "g", "h");
    PAssert.that(leftRow.apply("rows",SetFns.union(rightRow))).containsInAnyOrder(toRows("a", "b", "c", "d", "e", "f", "g", "h"));

    p.run();
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testUnionAll() {

    PAssert.that(left.apply("strings",SetFns.unionAll(right))).containsInAnyOrder(
            "a", "a", "a", "a", "a", "b", "b", "b", "b", "b", "c", "c", "d", "d", "d", "d", "e", "e", "f", "f", "g", "g", "h", "h");
    PAssert.that(leftRow.apply("rows", SetFns.unionAll(rightRow))).containsInAnyOrder(
            toRows("a", "a", "a", "a", "a", "b", "b", "b", "b", "b", "c", "c", "d", "d", "d", "d", "e", "e", "f", "f", "g", "g", "h", "h"));

    p.run();
  }
}
