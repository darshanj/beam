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
package org.apache.beam.sdk.transforms;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
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
  public void testCommons() {

    PCollection<String> left =
        p.apply(
            "left",
            Create.of("a", "a", "b", "b", "c", "c", "d", "d").withCoder(StringUtf8Coder.of()));

    PCollection<String> right =
        p.apply(
            "right",
            Create.of("a", "a", "b", "b", "e", "e", "f", "f").withCoder(StringUtf8Coder.of()));

    PCollectionList<String> commons = left.apply(SetFns.common(right));

    PAssert.that(commons.get(0)).containsInAnyOrder("a", "b");
    PAssert.that(commons.get(1)).containsInAnyOrder("c", "d");
    PAssert.that(commons.get(2)).containsInAnyOrder("e", "f");

    p.run();
  }

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
  public void testDifference() {

    PCollection<String> left =
        p.apply(
            "left",
            Create.of("a", "a", "b", "b", "c", "c", "d", "d").withCoder(StringUtf8Coder.of()));

    PCollection<String> right =
        p.apply(
            "right",
            Create.of("a", "a", "b", "b", "e", "e", "f", "f").withCoder(StringUtf8Coder.of()));

    PCollection<String> results = left.apply(SetFns.difference(right));

    PAssert.that(results).containsInAnyOrder("c", "d");

    p.run();
  }
}
