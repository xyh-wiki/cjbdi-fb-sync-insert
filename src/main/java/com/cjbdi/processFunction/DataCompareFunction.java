package com.cjbdi.processFunction;

import com.cjbdi.bean.IndexTableData;
import com.cjbdi.bean.SourceTableData;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class DataCompareFunction extends CoProcessFunction<SourceTableData, IndexTableData, String> {
    // 使用MapState来存储每个表的数据快照
    private transient MapState<String, String> sourceTableState;
    private transient MapState<String, String> indexTableState;
    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) {
        parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        MapStateDescriptor<String, String> sourceTableDesc = new MapStateDescriptor<>("sourceTableState", String.class, String.class);
        sourceTableState = getRuntimeContext().getMapState(sourceTableDesc);

        MapStateDescriptor<String, String> indexTableDesc = new MapStateDescriptor<>("indexTableState", String.class, String.class);
        indexTableState = getRuntimeContext().getMapState(indexTableDesc);
    }

    @Override
    public void processElement1(SourceTableData value, Context ctx, Collector<String> out) throws Exception {
        sourceTableState.put(value.getC_stm(), value.getD_xgsj());
        compareAndEmit(value, indexTableState, out, "SourceTable", "IndexTable");
    }

    @Override
    public void processElement2(IndexTableData value, Context ctx, Collector<String> out) throws Exception {
        indexTableState.put(value.getC_stm(), value.getD_xgsj());
        compareAndEmit(value, sourceTableState, out, "IndexTable", "SourceTable");
    }

    private void compareAndEmit(SourceTableData value, MapState<String, String> otherTableState, Collector<String> out, String currentTable, String otherTable) throws Exception {
        String otherUpdateFlag = otherTableState.get(value.getC_stm());
        if (otherUpdateFlag == null) {
            out.collect(value.getC_stm() + " exists in " + currentTable + " but not in " + otherTable);
        } else if (!value.getD_xgsj().equals(otherUpdateFlag)) {
            out.collect("Different update flags for ID " + value.getC_stm() + ": " + currentTable + "=" + value.getD_xgsj() + ", " + otherTable + "=" + otherUpdateFlag);
        }
    }

    private void compareAndEmit(IndexTableData value, MapState<String, String> otherTableState, Collector<String> out, String currentTable, String otherTable) throws Exception {
        String otherUpdateFlag = otherTableState.get(value.getC_stm());
        if (otherUpdateFlag == null) {
            out.collect(value.getC_stm() + " exists in " + currentTable + " but not in " + otherTable);
        } else if (!value.getD_xgsj().equals(otherUpdateFlag)) {
            out.collect("Different update flags for ID " + value.getC_stm() + ": " + currentTable + "=" + value.getD_xgsj() + ", " + otherTable + "=" + otherUpdateFlag);
        }
    }
}
