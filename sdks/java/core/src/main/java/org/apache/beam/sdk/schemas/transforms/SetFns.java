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

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

public class SetFns {

  public static <T> SetImpl<T> intersect(PCollection<T> rightCollection) {
    SerializableBiFunction<Long, Long, Long> intersectFn =
        (inFirst, inSecond) -> (inFirst > 0 && inSecond > 0) ? 1L : 0L;
    return new SetImpl<>(rightCollection, intersectFn);
  }

  public static <T> SetImpl<T> intersectAll(PCollection<T> rightCollection) {
    SerializableBiFunction<Long, Long, Long> intersectFn =
        (inFirst, inSecond) -> (inFirst > 0 && inSecond > 0) ? Math.min(inFirst, inSecond) : 0L;
    return new SetImpl<>(rightCollection, intersectFn);
  }

  public static <T> SetImpl<T> except(PCollection<T> rightCollection) {
    SerializableBiFunction<Long, Long, Long> exceptFn =
        (inFirst, inSecond) -> inFirst > 0 && inSecond == 0 ? 1L : 0L;
    return new SetImpl<>(rightCollection, exceptFn);
  }

  public static <T> SetImpl<T> exceptAll(PCollection<T> rightCollection) {
    SerializableBiFunction<Long, Long, Long> exceptFn =
        (inFirst, inSecond) -> {
          if (inFirst > 0 && inSecond == 0) {
            return inFirst;
          } else if (inFirst > 0 && inSecond > 0) {
            return Math.max(inFirst - inSecond, 0L);
          }
          return 0L;
        };
    return new SetImpl<>(rightCollection, exceptFn);
  }

  public static <T> SetImpl<T> unionAll(PCollection<T> rightCollection) {
    SerializableBiFunction<Long, Long, Long> unionFn = Long::sum;
    return new SetImpl<>(rightCollection, unionFn);
  }

  public static <T> SetImpl<T> union(PCollection<T> rightCollection) {
    SerializableBiFunction<Long, Long, Long> unionFn = (inFirst, inSecond) -> 1L;
    return new SetImpl<>(rightCollection, unionFn);
  }

  private static <T> PCollection<T> findComms(
      PCollection<T> leftCollection,
      PCollection<T> rightCollection,
      SerializableBiFunction<Long, Long, Long> fn) {

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

    PCollection<KV<T, Void>> left =
        leftCollection.apply("left collection to KV of elem and Void", elementToVoid);
    PCollection<KV<T, Void>> right =
        rightCollection.apply("right collection to KV of elem and Void", elementToVoid);

    PCollection<KV<T, CoGbkResult>> coGbkResults =
        KeyedPCollectionTuple.of(leftCollectionTag, left)
            .and(rightCollectionTag, right)
            .apply(CoGroupByKey.create());

    return coGbkResults.apply(
        ParDo.of(
            new DoFn<KV<T, CoGbkResult>, T>() {

              @ProcessElement
              public void processElement(ProcessContext c) {
                KV<T, CoGbkResult> elementGroups = c.element();

                CoGbkResult value = elementGroups.getValue();
                long inFirstSize = Iterables.size(value.getAll(leftCollectionTag));
                long inSecondSize = Iterables.size(value.getAll(rightCollectionTag));

                T element = elementGroups.getKey();
                for (long i = 0L; i < fn.apply(inFirstSize, inSecondSize); i++) {
                  c.output(element);
                }
              }
            }));
  }

  public static class SetImpl<T> extends PTransform<PCollection<T>, PCollection<T>> {
    private final PCollection<T> rightCollection;
    private final SerializableBiFunction<Long, Long, Long> fn;

    private SetImpl(PCollection<T> rightCollection, SerializableBiFunction<Long, Long, Long> fn) {
      this.rightCollection = rightCollection;
      this.fn = fn;
    }

    @Override
    public PCollection<T> expand(PCollection<T> leftCollection) {
      return findComms(leftCollection, rightCollection, fn).setCoder(leftCollection.getCoder());
    }
  }
}
