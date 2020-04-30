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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

public class SetFns {

  public static <T> Common<T> common(PCollection<T> rightCollection) {
    return new Common<>(rightCollection);
  }

  public static <T> Intersect<T> intersect(PCollection<T> rightCollection) {
    return new Intersect<>(rightCollection);
  }

  private static <T> PCollectionList<T> findComms(
      PCollection<T> rightCollection, PCollection<T> leftCollection) {
    TupleTag<Void> leftCollectionTag = new TupleTag<>();
    TupleTag<Void> rightCollectionTag = new TupleTag<>();

    MapElements<T, KV<T, Void>> elementToVoid =
        MapElements.via(
            new SimpleFunction<T, KV<T, Void>>() {
              @Override
              public KV<T, Void> apply(T element) {
                return KV.of(element, null);
              }
            });

    PCollection<KV<T, Void>> left = leftCollection.apply(elementToVoid);
    PCollection<KV<T, Void>> right = rightCollection.apply(elementToVoid);

    PCollection<KV<T, CoGbkResult>> coGbkResults =
        KeyedPCollectionTuple.of(leftCollectionTag, left)
            .and(rightCollectionTag, right)
            .apply(CoGroupByKey.create());

    TupleTag<T> matched = new TupleTag<>("matched");
    TupleTag<T> onlyInLeft = new TupleTag<>("onlyInLeft");
    TupleTag<T> onlyInRight = new TupleTag<>("onlyInRight");
    Coder<T> coder = leftCollection.getCoder();

    PCollectionTuple results =
        coGbkResults.apply(
            ParDo.of(
                    new DoFn<KV<T, CoGbkResult>, T>() {
                      @ProcessElement
                      public void processElement(ProcessContext c) {
                        KV<T, CoGbkResult> elementGroups = c.element();

                        CoGbkResult value = elementGroups.getValue();
                        boolean inFirst = Iterables.isEmpty(value.getAll(leftCollectionTag));
                        boolean inSecond = Iterables.isEmpty(value.getAll(rightCollectionTag));

                        T element = elementGroups.getKey();

                        if (inFirst && !inSecond) {
                          c.output(onlyInLeft, element);
                        } else if (!inFirst && inSecond) {
                          c.output(onlyInRight, element);
                        } else {
                          c.output(matched, element);
                        }
                      }
                    })
                .withOutputTags(matched, TupleTagList.of(onlyInLeft).and(onlyInRight)));

    return PCollectionList.of(results.get(matched).setCoder(coder))
        .and(results.get(onlyInLeft).setCoder(coder))
        .and(results.get(onlyInRight).setCoder(coder));
  }

  public static class Intersect<T> extends PTransform<PCollection<T>, PCollection<T>> {
    private final PCollection<T> rightCollection;

    private Intersect(PCollection<T> rightCollection) {
      this.rightCollection = rightCollection;
    }

    @Override
    public PCollection<T> expand(PCollection<T> leftCollection) {
      PCollectionList<T> results = findComms(leftCollection, rightCollection);
      return results.get(0);
    }
  }

  public static class Difference<T> extends PTransform<PCollection<T>, PCollection<T>> {
    private final PCollection<T> rightCollection;

    private Difference(PCollection<T> rightCollection) {
      this.rightCollection = rightCollection;
    }

    @Override
    public PCollection<T> expand(PCollection<T> leftCollection) {
      PCollectionList<T> results = findComms(leftCollection, rightCollection);
      return results.get(1);
    }
  }

  public static class Common<T> extends PTransform<PCollection<T>, PCollectionList<T>> {
    private final PCollection<T> rightCollection;

    private Common(PCollection<T> rightCollection) {
      this.rightCollection = rightCollection;
    }

    @Override
    public PCollectionList<T> expand(PCollection<T> leftCollection) {
      return findComms(leftCollection, rightCollection);
    }
  }
}
