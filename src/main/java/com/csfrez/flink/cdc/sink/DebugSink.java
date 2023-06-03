package com.csfrez.flink.cdc.sink;

import com.csfrez.flink.cdc.bean.StatementBean;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


public class DebugSink  extends RichSinkFunction<StatementBean> {
    private String active = "";

    public DebugSink(String active){
        this.active = active;
    }

    // 每来一条数据，print
    @Override
    public void invoke(StatementBean value, Context context) throws Exception {
        System.out.println(value);
        System.out.println(context);
        System.out.println("操作类型为===>>" + value.getOperationType());
        System.out.println("SQL===>>" + value.getSql());
    }

}
