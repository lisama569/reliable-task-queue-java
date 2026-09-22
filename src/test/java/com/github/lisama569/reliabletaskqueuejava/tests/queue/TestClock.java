package com.github.lisama569.reliabletaskqueuejava.tests.queue;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/** 可控时钟：测试中可显式前进时间，用于确定性地验证退避与租约过期。 */
public class TestClock extends Clock {

    private long millis;

    public TestClock(long millis) {
        this.millis = millis;
    }

    public void setMillis(long millis) {
        this.millis = millis;
    }

    public void advance(long deltaMillis) {
        this.millis += deltaMillis;
    }

    @Override
    public long millis() {
        return millis;
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(millis);
    }
}
