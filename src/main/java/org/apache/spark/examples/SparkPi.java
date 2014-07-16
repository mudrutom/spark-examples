/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Computes an approximation to pi
 * Usage: SparkPi [slices]
 */
public final class SparkPi {

	public static void main(String[] args) throws Exception {
		final SparkConf sparkConf = new SparkConf().setAppName("SparkPi");
		final JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		final int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
		final int n = 100000 * slices;
		final List<Integer> l = new ArrayList<>(n);
		for (int i = 0; i < n; i++) {
			l.add(i);
		}

		final JavaRDD<Integer> dataSet = jsc.parallelize(l, slices);

		final int count = dataSet.map(integer -> {
			double x = Math.random() * 2 - 1;
			double y = Math.random() * 2 - 1;
			return (x * x + y * y < 1) ? 1 : 0;
		}).reduce((a, b) -> a + b);

		System.out.println("Pi is roughly " + 4.0 * count / n);
	}
}
