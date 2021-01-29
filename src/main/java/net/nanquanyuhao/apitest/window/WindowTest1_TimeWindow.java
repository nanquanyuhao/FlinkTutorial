package net.nanquanyuhao.apitest.window;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTutorial
 * Package: com.atguigu.apitest.window
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/9 14:37
 */

import net.nanquanyuhao.apitest.beans.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName: WindowTest1_TimeWindow
 * @Description:
 * @Author: wushengran on 2020/11/9 14:37
 * @Version: 1.0
 */
public class WindowTest1_TimeWindow {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 重启策略为固定延时重启，如果发生故障，系统会尝试重新启动作业5次，并在连续重启尝试之间等待50秒
        // env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, 50000L));
        env.setParallelism(1);

//        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("D:\\code\\job\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        // socket文本流
        // DataStream<String> inputStream = env.socketTextStream("192.168.235.101", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.assignTimestampsAndWatermarks(new WatermarkStrategy<SensorReading>()).map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 开窗测试
        // 1. 增量聚合函数
        DataStream<Integer> resultStream = dataStream.keyBy(new KeySelector<SensorReading, String>() {

            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.getId();
            }
        })
//                .countWindow(10, 2);
//                .window(EventTimeSessionWindows.withGap(Time.minutes(1)));
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                // 滚动时间窗口 15s
                .timeWindow(Time.seconds(15))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {

                    // 创建累加器
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    // 实现累加逻辑
                    @Override
                    public Integer add(SensorReading value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    // 获取累加后结果
                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    // 一般用于 session window 的合并
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                });

        resultStream.print();

        // 2. 全窗口函数
        /*SingleOutputStreamOperator<Tuple3<String, Long, Integer>> resultStream2 = dataStream.keyBy(new KeySelector<SensorReading, String>() {

            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.getId();
            }
        })
                .timeWindow(Time.seconds(15))*/
//                .process(new ProcessWindowFunction<SensorReading, Object, Tuple, TimeWindow>() {
//                })
                /*.apply(new WindowFunction<SensorReading, Tuple3<String, Long, Integer>, String, TimeWindow>() {

                    *//**
                     *
                     * @param s 分组的 key
                     * @param window
                     * @param input
                     * @param out
                     * @throws Exception
                     *//*
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                        String id = s;
                        Long windowEnd = window.getEnd();
                        Integer count = IteratorUtils.toList(input.iterator()).size();
                        out.collect(new Tuple3<>(id, windowEnd, count));
                    }
                });
*/
        // 3. 其它可选API
        /*OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        SingleOutputStreamOperator<SensorReading> sumStream = dataStream.keyBy(new KeySelector<SensorReading, String>() {

            @Override
            public String getKey(SensorReading sensorReading) throws Exception {
                return sensorReading.getId();
            }
        })
                .timeWindow(Time.seconds(15))
//                .trigger()
//                .evictor()
                // 允许迟到的数据时间，只针对事件时间语义下有效
                .allowedLateness(Time.minutes(1))
                // 超过迟到时间的迟到数据放入侧输出流
                .sideOutputLateData(outputTag)
                .sum("temperature");

        // sumStream.getSideOutput(outputTag).print("late");
        // resultStream2.print();
*/
        env.execute();
    }
}
