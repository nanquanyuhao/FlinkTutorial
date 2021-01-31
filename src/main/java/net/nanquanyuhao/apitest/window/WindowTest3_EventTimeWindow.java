package net.nanquanyuhao.apitest.window;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTutorial
 * Package: com.atguigu.apitest.window
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/10 9:33
 */

import net.nanquanyuhao.apitest.beans.SensorReading;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName: WindowTest3_EventTimeWindow
 * @Description:
 * @Author: wushengran on 2020/11/10 9:33
 * @Version: 1.0
 */
public class WindowTest3_EventTimeWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 默认就是事件时间，1.10.1 的版本还是处理时间为默认时间
        // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // Watermark 自动生成的时间间隔——大数据情况下周期性生成，数据少的建议每个生成，此处每 100ms 生成一次 Watermark
        env.getConfig().setAutoWatermarkInterval(100);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("192.168.235.101", 7777);

        // 转换成SensorReading类型，分配时间戳和watermark
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0].trim(), new Long(fields[1]), new Double(fields[2]));
        })

                // 升序数据设置事件时间和watermark
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
//                    @Override
//                    public long extractAscendingTimestamp(SensorReading element) {
//                        return element.getTimestamp() * 1000L;
//                    }
//                })
                // 乱序数据设置时间戳和watermark
                .assignTimestampsAndWatermarks(
                        // Watermarks 延时时间为 2s
                        new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {

                            @Override
                            public long extractTimestamp(SensorReading element) {
                                // ms 数返回的时间戳
                                return element.getTimestamp() * 1000L;
                            }
                        });

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        // 基于事件时间的开窗聚合，统计15秒内温度的最小值
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy(new KeySelector<SensorReading, String>() {

            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.getId();
            }
        })
                // .timeWindow(Time.seconds(15))
                .window(TumblingEventTimeWindows.of(Time.seconds(15)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .minBy("temperature");

        minTempStream.print("minTemp");
        minTempStream.getSideOutput(outputTag).print("late");

        env.execute();
    }
}
