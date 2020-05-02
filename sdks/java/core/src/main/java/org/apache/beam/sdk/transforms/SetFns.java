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

import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

public class SetFns {

    public static <T> SetImpl<T> intersect(PCollection<T> rightCollection) {
        SerializableBiFunction<Boolean, Boolean, Boolean> intersectFn = (inFirst, inSecond) -> ((inFirst && inSecond) || (!inFirst && !inSecond));
        return new SetImpl<T>(rightCollection,intersectFn);
    }

    public static <T> SetImpl<T> except(PCollection<T> rightCollection) {
        SerializableBiFunction<Boolean, Boolean, Boolean> exceptFn = (inFirst, inSecond) -> inFirst && !inSecond;
        return new SetImpl<T>(rightCollection,exceptFn);
    }

    public static <T> SetImpl<T> union(PCollection<T> rightCollection) {
        SerializableBiFunction<Boolean, Boolean, Boolean> unionFn = (inFirst, inSecond) -> true;
        return new SetImpl<T>(rightCollection,unionFn);
    }

    private static <T> PCollection<T> findComms(PCollection<T> leftCollection, PCollection<T> rightCollection, SerializableBiFunction<Boolean, Boolean, Boolean> fn) {

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

        PCollection<KV<T, Void>> left = leftCollection.apply("Prepare left collection for Grouping", elementToVoid);
        PCollection<KV<T, Void>> right = rightCollection.apply("Prepare right collection for Grouping", elementToVoid);

        PCollection<KV<T, CoGbkResult>> coGbkResults =
                KeyedPCollectionTuple.of(leftCollectionTag, left)
                        .and(rightCollectionTag, right)
                        .apply(CoGroupByKey.create());

        return coGbkResults.apply(
                ParDo.of(new DoFn<KV<T, CoGbkResult>, T>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<T, CoGbkResult> elementGroups = c.element();

                        CoGbkResult value = elementGroups.getValue();
                        boolean inFirst = Iterables.isEmpty(value.getAll(leftCollectionTag));
                        boolean inSecond = Iterables.isEmpty(value.getAll(rightCollectionTag));

                        T element = elementGroups.getKey();
                        if(fn.apply(inFirst, inSecond)) {
                            c.output(element);
                        }
                    }
                }));
    }

    public static class SetImpl<T> extends PTransform<PCollection<T>, PCollection<T>> {
        private final PCollection<T> rightCollection;
        private SerializableBiFunction<Boolean, Boolean, Boolean> fn;

        private SetImpl(PCollection<T> rightCollection, SerializableBiFunction<Boolean, Boolean, Boolean> fn) {
            this.rightCollection = rightCollection;
            this.fn = fn;
        }

        @Override
        public PCollection<T> expand(PCollection<T> leftCollection) {
            return findComms(leftCollection, rightCollection,fn)
                    .setCoder(leftCollection.getCoder());
        }
    }
}