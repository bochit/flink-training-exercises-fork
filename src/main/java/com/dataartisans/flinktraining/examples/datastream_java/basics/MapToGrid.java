package com.dataartisans.flinktraining.examples.datastream_java.basics;


import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.EnrichedRide;
import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.joda.time.Interval;
import org.joda.time.Minutes;

public class MapToGrid {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToRideData);

        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minutes are served every second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor));

        // filter and map input
//        DataStream<EnrichedRide> enrichedNYCRides = rides
//                .filter(ride -> GeoUtils.isInNYC(ride.startLon,ride.startLat) &&  GeoUtils.isInNYC(ride.endLon,ride.endLat))
//                .map(new Enrichment());
//        enrichedNYCRides.print();

        // filter and map in one step with flatMap
        DataStream<EnrichedRide> enrichedNYCRides = rides
                .flatMap(new NYCEnrichment())
                .keyBy(ride -> ride.startCell);

//        enrichedNYCRides.print();

        DataStream<Tuple3<Integer,Integer, Minutes>> minutesByStartCell = enrichedNYCRides
                .flatMap(new FlatMapFunction<EnrichedRide, Tuple3<Integer,Integer, Minutes>>() {
                    @Override
                    public void flatMap(EnrichedRide ride,
                                        Collector<Tuple3<Integer, Integer, Minutes>> out) throws Exception {
                        if (!ride.isStart) {
                            Interval rideInterval = new Interval(ride.startTime, ride.endTime);
                            Minutes duration = rideInterval.toDuration().toStandardMinutes();
                            out.collect(new Tuple3<>(ride.startCell, ride.endCell,duration));
                        }
                    }
                });
//                .flatMap((ride,out)->{
//                    if (!ride.isStart) {
//                            Interval rideInterval = new Interval(ride.startTime, ride.endTime);
//                            Minutes duration = rideInterval.toDuration().toStandardMinutes();
//                            out.collect(new Tuple2<>(ride.startCell, duration));
//                    }
//                    return;
//                });

        minutesByStartCell
                .keyBy(0) // startCell
                .maxBy(2) // duration
//                .max(2) // duration
                .print();

        // run the cleansing pipeline
        env.execute();
    }

    public static class NYCEnrichment implements FlatMapFunction<TaxiRide, EnrichedRide> {
        @Override
        public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> out) throws Exception {
            FilterFunction<TaxiRide> valid = (ride -> GeoUtils.isInNYC(ride.startLon,ride.startLat) &&  GeoUtils.isInNYC(ride.endLon,ride.endLat));
            if (valid.filter(taxiRide)) {
                out.collect(new EnrichedRide(taxiRide));
            }
        }
    }


    public static class Enrichment implements MapFunction<TaxiRide, EnrichedRide> {
        @Override
        public EnrichedRide map(TaxiRide taxiRide) throws Exception {
            return new EnrichedRide(taxiRide);
        }
    }
}
