package com.atguigu.gmall.realtime.dwd.log.split;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwdBaseLog().start(10011,4,"dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag"){};

        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);

                            out.collect(jsonObj);
                        } catch (Exception e){
                            ctx.output(dirtyTag,jsonStr);
                        }
                    }
                }
        );
//        jsonObjDs.print("标准的json");
        SideOutputDataStream<String> dirtyDS = jsonObjDs.getSideOutput(dirtyTag);
//        dirtyDS.print("脏数据");

        //将侧输出流中的脏数据写到kafka主题中
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("dirty_data")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                //当前配置决定是否开启事务，保证写到kafka数据的精准一次
//                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
//                .setTransactionalIdPrefix("dwd_base_log_")
//                //设置事务超时时间   检查点超时间见长
//                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"")
                .build();
        dirtyDS.sinkTo(kafkaSink);

        //TODD  对新老访客标记进行修复
        //按照设备id进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDs.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> fixedDS = keyedDS.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> lastVistDateState;

            @Override
            public void open(Configuration parameters) throws Exception {

                ValueStateDescriptor<String> valueStateDescriptor
                        = new ValueStateDescriptor<String>("lastVistDateState", String.class);

                lastVistDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public JSONObject map(JSONObject jsonObj) throws Exception {

                String isNew = jsonObj.getJSONObject("common").getString("is_new");

                String lastVisitDate = lastVistDateState.value();

                Long ts = jsonObj.getLong("ts");

                String curVisitDate = DateFormatUtil.tsToDate(ts);

                if ("1".equals(isNew)) {
                    if (StringUtils.isEmpty(lastVisitDate)) {
                        lastVistDateState.update(curVisitDate);
                    } else {
                        if (!lastVisitDate.equals(curVisitDate)) {
                            isNew = "0";
                            jsonObj.getJSONObject("common").put("is_new", isNew);
                        }
                    }
                } else {
                    if (StringUtils.isEmpty(lastVisitDate)) {
                        String yesterDay = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                        lastVistDateState.update(yesterDay);
                    }
                }

                return jsonObj;
            }
        });
//        新老用户修改输出
//        fixedDS.print();

        //TODD  分流  错误日志 - 错误侧输出流  启动日志 - 启动侧输出流  曝光日志 - 曝光侧输出流  动作日志 - 动作侧输出流  页面日志 - 主流
        OutputTag<String> errTag = new OutputTag<String>("errorTag") {};

        OutputTag<String> startTag = new OutputTag<String>("startTag") {};

        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};

        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};

        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        //错误日志
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            ctx.output(errTag, jsonObj.toJSONString());
                        }

                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        if (startJsonObj != null) {
                            //启动日志
                            ctx.output(startTag, jsonObj.toJSONString());
                        } else {
                            //页面日志
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");

                            //曝光日志
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                for (int i = 0; i < displayArr.size(); i++) {

                                    JSONObject displayJsonObj = displayArr.getJSONObject(i);

                                    JSONObject newDisplayJsonobj = new JSONObject();
                                    newDisplayJsonobj.put("common", commonJsonObj);
                                    newDisplayJsonobj.put("page", pageJsonObj);
                                    newDisplayJsonobj.put("display", displayJsonObj);
                                    newDisplayJsonobj.put("ts", ts);

                                    ctx.output(displayTag, newDisplayJsonobj.toJSONString());
                                }
                                jsonObj.remove("displays");
                            }
                            //动作日志
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            for (int i = 0; i < actionArr.size(); i++) {
                                JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                JSONObject newActionJsonObj = new JSONObject();
                                newActionJsonObj.put("common", commonJsonObj);
                                newActionJsonObj.put("page", pageJsonObj);
                                newActionJsonObj.put("action", actionJsonObj);
                                ctx.output(actionTag, newActionJsonObj.toJSONString());
                            }
                            jsonObj.remove("actions");
                        }

                        //页面日志，写到主流中
                        out.collect(jsonObj.toJSONString());
                    }
                }
        );

        //TODD将不同流的数据写到KAFKA不同主题中
        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        pageDS.print("页面");
        errDS.print("错误");
        startDS.print("启动");
        displayDS.print("曝光");
        actionDS.print("动作");

        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        errDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        startDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        displayDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }
}
