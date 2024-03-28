package com.cjbdi.trigger;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.io.IOException;

public class CountOrTimeTrigger extends Trigger<Object, TimeWindow> {
    private final long maxCount; // 触发的最大计数
    private final long interval; // 时间间隔

    private CountOrTimeTrigger(long maxCount, long interval) {
        this.maxCount = maxCount;
        this.interval = interval;
    }

    public static CountOrTimeTrigger of(long maxCount, long interval) {
        return new CountOrTimeTrigger(maxCount, interval);
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        // 注册系统时间回调
        ctx.registerProcessingTimeTimer(window.maxTimestamp());

        // 累积计数
        TriggerContextState state = new TriggerContextState(ctx);
        long count = state.increaseCount();
        
        if (count >= maxCount) {
            state.clear();
            return TriggerResult.FIRE;
        }

        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        TriggerContextState state = new TriggerContextState(ctx);
        state.clear();
    }

    // 辅助类，用于处理触发器状态
    private static class TriggerContextState {
        private final TriggerContext ctx;
        private static final String COUNT_STATE = "count";

        private TriggerContextState(TriggerContext ctx) {
            this.ctx = ctx;
        }

        public long increaseCount() throws IOException {
            ValueState<Long> state = ctx.getPartitionedState(
                new ValueStateDescriptor<>(COUNT_STATE, Long.class));
            Long currentValue = state.value();
            if (currentValue == null) {
                currentValue = 1L;
            } else {
                currentValue += 1;
            }
            state.update(currentValue);
            return currentValue;
        }

        public void clear() throws Exception {
            ValueState<Long> state = ctx.getPartitionedState(
                new ValueStateDescriptor<>(COUNT_STATE, Long.class));
            state.clear();
        }
    }
}
