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
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;

import java.io.Serializable;

public class SetFns {

    public static <T> Impl<T, PCollectionList<T>> common(PCollection<T> rightCollection) {
        return new Impl<T,PCollectionList<T>>(rightCollection, new CommonsFn<T>() { }) { };
    }

    public static <T> Impl<T, PCollection<T>> intersect(PCollection<T> rightCollection) {
        return new Impl<T,PCollection<T>>(rightCollection, new IntersectFn<T>() { }) { };
    }

    public static <T> Impl<T, PCollection<T>> difference(PCollection<T> rightCollection) {
        return new Impl<T,PCollection<T>>(rightCollection, new DifferenceFn<T>() { }) { };
    }
    public interface CalculateFn<T,R extends POutput> extends Serializable {
        TupleTagList getOutputTags();

        void calculateFor(boolean inFirst, boolean inSecond, DoFn.ProcessContext c,T element);
        R calculateResults(Coder<T> coder,PCollectionTuple tagged);
    }

    public abstract static class CommonsFn<T> implements CalculateFn<T,PCollectionList<T>> {
        private final TupleTag<T> matched = new TupleTag<>("matched");
        private final TupleTag<T> onlyInLeft = new TupleTag<>("onlyInLeft");
        private final TupleTag<T> onlyInRight = new TupleTag<>("onlyInRight");

        @Override
        public TupleTagList getOutputTags() {
            return TupleTagList.of(matched)
                    .and(onlyInLeft)
                    .and(onlyInRight);
        }

        @Override
        public void calculateFor(boolean inFirst, boolean inSecond, DoFn.ProcessContext c, T element) {
            if (inFirst && !inSecond) {
                c.output(onlyInLeft, element);
            } else if (!inFirst && inSecond) {
                c.output(onlyInRight, element);
            } else {
                c.output(matched, element);
            }
        }

        @Override
        public PCollectionList<T> calculateResults(Coder<T> coder, PCollectionTuple results) {
            return PCollectionList.of(results.get(matched).setCoder(coder))
                    .and(results.get(onlyInLeft).setCoder(coder))
                    .and(results.get(onlyInRight).setCoder(coder));
        }
    }

    public abstract static class IntersectFn<T> implements CalculateFn<T,PCollection<T>> {
        private final TupleTag<T> matched = new TupleTag<>("matched");

        @Override
        public TupleTagList getOutputTags() {
            return TupleTagList.of(matched);
        }

        @Override
        public void calculateFor(boolean inFirst, boolean inSecond, DoFn.ProcessContext c, T element) {
            if (inFirst && inSecond) {
                c.output(matched, element);
            }
        }

        @Override
        public PCollection<T> calculateResults(Coder<T> coder, PCollectionTuple results) {
            return results.get(matched).setCoder(coder);
        }
    }

    public abstract static class DifferenceFn<T> implements CalculateFn<T,PCollection<T>> {
        private final TupleTag<T> onlyInLeft = new TupleTag<>("onlyInLeft");

        @Override
        public TupleTagList getOutputTags() {
            return TupleTagList.of(onlyInLeft);
        }

        @Override
        public void calculateFor(boolean inFirst, boolean inSecond, DoFn.ProcessContext c, T element) {
            if (inFirst && !inSecond) {
                c.output(onlyInLeft, element);
            }
        }

        @Override
        public PCollection<T> calculateResults(Coder<T> coder, PCollectionTuple results) {
            return results.get(onlyInLeft).setCoder(coder);
        }
    }
public static class ElementPresence<T> implements Serializable {
    private T elem;
    private boolean inFirst;
    private boolean inSecond;

    public ElementPresence(T elem, boolean inFirst, boolean inSecond) {
        this.elem = elem;
        this.inFirst = inFirst;
        this.inSecond = inSecond;
    }

    public T getElem() {
        return elem;
    }

    public boolean isInFirst() {
        return inFirst;
    }

    public boolean isInSecond() {
        return inSecond;
    }
}

    public abstract static class Impl<T,OutputT extends POutput> extends PTransform<PCollection<T>,OutputT> {
        private final PCollection<T> rightCollection;
        private final SetFns.CalculateFn<T,OutputT> commonsFn;

        private Impl(PCollection<T> rightCollection, CalculateFn<T,OutputT> commonsFn) {
            this.rightCollection = rightCollection;
            this.commonsFn = commonsFn;
        }

        @Override
        public OutputT expand(PCollection<T> leftCollection) {

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

            PCollection<KV<T, Void>> left = leftCollection.apply("left collection to KV of elem and Void", elementToVoid);
            PCollection<KV<T, Void>> right = rightCollection.apply("right collection to KV of elem and Void", elementToVoid);

            PCollection<KV<T, CoGbkResult>> coGbkResults =
                    KeyedPCollectionTuple.of(leftCollectionTag, left)
                            .and(rightCollectionTag, right)
                            .apply(CoGroupByKey.create());

            Coder<T> coder = leftCollection.getCoder();
            PCollectionTuple results =
                    coGbkResults.apply(
                            ParDo.of(new DoFn<KV<T, CoGbkResult>, Void>() {

                                        @ProcessElement
                                        public void processElement(ProcessContext c) {
                                            KV<T, CoGbkResult> elementGroups = c.element();

                                            CoGbkResult value = elementGroups.getValue();
                                            boolean inFirst = Iterables.isEmpty(value.getAll(leftCollectionTag));
                                            boolean inSecond = Iterables.isEmpty(value.getAll(rightCollectionTag));

                                            T element = elementGroups.getKey();
                                            commonsFn.calculateFor(inFirst, inSecond,c,element);
                                        }
                                    })
                                    .withOutputTags(new TupleTag<Void>() {},commonsFn.getOutputTags()));

            return commonsFn.calculateResults(coder,results);
        }
    }
}
