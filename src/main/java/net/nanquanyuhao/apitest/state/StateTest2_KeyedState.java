package net.nanquanyuhao.apitest.state;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTutorial
 * Package: com.atguigu.apitest.state
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/10 15:49
 */

import net.nanquanyuhao.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: StateTest2_KeyedState
 * @Description:
 * @Author: wushengran on 2020/11/10 15:49
 * @Version: 1.0
 */
public class StateTest2_KeyedState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("192.168.235.101", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个有状态的map操作，统计当前sensor数据个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream
                .keyBy(new KeySelector<SensorReading, String>() {
                    @Override
                    public String getKey(SensorReading sensorReading) throws Exception {
                        return sensorReading.getId();
                    }
                })
                .map(new MyKeyCountMapper());

        resultStream.print();

        env.execute();
    }

    // 自定义RichMapFunction，因为需要获取当前上下文
    public static class MyKeyCountMapper extends RichMapFunction<SensorReading, Integer> {

        // 声明一个键控状态
        private ValueState<Integer> keyCountState;

        // 其它类型状态的声明
        private ListState<String> myListState;
        private MapState<String, Double> myMapState;
        private ReducingState<SensorReading> myReducingState;

        @Override
        public void open(Configuration parameters) throws Exception {

            keyCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Integer>("key-count", Integer.class));

            myListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<String>("my-list", String.class));
            myMapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<String, Double>("my-map", String.class, Double.class));
//            myReducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<SensorReading>())
        }

        @Override
        public Integer map(SensorReading value) throws Exception {
            // 其它状态API调用
            // list state
            for (String str : myListState.get()) {
                System.out.println(str);
            }
            myListState.add("hello");

            // map state
            myMapState.get("1");
            myMapState.put("2", 12.3);
            myMapState.remove("2");
            // reducing state
//            myReducingState.add(value);
            myMapState.clear();

            // 每个键组数据个数计数，初始值未赋值，需要额外判断处理其值
            Integer count = keyCountState.value() == null ? 0 : keyCountState.value();
            count++;
            keyCountState.update(count);

            return count;
        }
    }
}
