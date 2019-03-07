/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.exercises.datastream_java.windows;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.MissingSolutionException;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * The "Popular Places" exercise of the Flink training
 * (http://training.data-artisans.com).
 *
 * The task of the exercise is to identify every five minutes popular areas where many taxi rides
 * arrived or departed in the last 15 minutes.
 *
 * Parameters:
 * -input path-to-input-file
 *
 */

// random comment testing commitr
public class PopularPlacesExercise extends ExerciseBase {
	public static void main(String[] args) throws Exception {

		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToRideData);
		final int popThreshold = params.getInt("threshold", 20);

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		// set up streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		// start the data generator
		DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

		// find n most popular spots
		DataStream<?> popularPlaces = rides
				// remove all rides which are not within NYC
				.filter(new NYCFilter())
				// match ride to grid cell and event type (start or end)
				.map(new GridCellMatcher())
				.keyBy(0,1)
				.timeWindow(Time.minutes(15), Time.minutes(5))
				.process(new rideCounter())
				.filter(count -> count.f2 >= popThreshold )
				.map(new mapToPoint());

		printOrTest(popularPlaces);

		env.execute("Popular Places");
	}

	public static class mapToPoint implements MapFunction<
			Tuple4<Integer, Boolean, Integer, Long>,
			Tuple5<Float, Float, Long, Boolean, Integer>
			> {
		@Override
		public Tuple5<Float, Float, Long, Boolean, Integer> map(Tuple4<Integer, Boolean, Integer, Long> input) throws Exception {
//			throw new MissingSolutionException();
			float pointLon = GeoUtils.getGridCellCenterLon(input.f0);
			float pointLat = GeoUtils.getGridCellCenterLat(input.f0);

			return new Tuple5<>(pointLon,
					pointLat,
					input.f3,
					input.f1,
					input.f2);
		}
	}

	public static class rideCounter extends ProcessWindowFunction<
			Tuple2<Integer, Boolean>,
			Tuple4<Integer, Boolean, Integer, Long>,
			Tuple,
			TimeWindow> {
		@Override
		public void process(Tuple key,
							Context context, Iterable<Tuple2<Integer, Boolean>> rides,
							Collector<Tuple4<Integer, Boolean, Integer, Long>> out) throws Exception {
			int count = 0;
			for (Tuple2<Integer, Boolean> ride : rides) {
				count++;
			}

			out.collect(new Tuple4<>(key.getField(0), key.getField(1),count, context.window().getEnd() ));
		}
	}


	/**
	 * Map taxi ride to grid cell and event type.
	 * Start records use departure location, end record use arrival location.
	 */
	public static class GridCellMatcher implements MapFunction<TaxiRide, Tuple2<Integer, Boolean>> {

		@Override
		public Tuple2<Integer, Boolean> map(TaxiRide taxiRide) throws Exception {
//			throw new MissingSolutionException();

			int gridId = GeoUtils.mapToGridCell(taxiRide.isStart? taxiRide.startLon:taxiRide.endLon,
					taxiRide.isStart? taxiRide.startLat:taxiRide.endLat);
			return new Tuple2<>(gridId, taxiRide.isStart);
		}
	}

	public static class NYCFilter implements FilterFunction<TaxiRide> {
		@Override
		public boolean filter(TaxiRide taxiRide) throws Exception {

			return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) &&
					GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
		}
	}
}
