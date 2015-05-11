package net.lenards.types;

import java.io.Serializable;

public class EventRecord implements Serializable {
    public String key;
    public String cmpkey;
    public Long ts;
    public String event;
    public int num;

    public EventRecord() {
        this("", "", 0L, "0", 0);
    }

    public EventRecord(String key, String cmpkey, Long ts, String event, int count) {
        this.key = key;
        this.cmpkey = cmpkey;
        this.ts = ts;
        this.event = event;
        this.num = count;
    }

    public String getKey() {
        return this.key;
    }

    public void setKey(String k) {
        this.key = k;
    }

    public String getCmpkey() {
        return this.cmpkey;
    }

    public void setCmpkey(String ck) {
        this.cmpkey = ck;
    }

    public long getTs() {
        return this.ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public String getEvent() {
        return this.event;
    }

    public void setEvent(String event) {
        this.event = event;
    }

    public int getNum() {
        return this.num;
    }

    public void setNum(int n) {
        this.num = n;
    }

    @Override
    public String toString() {
        return String.format("EventRecord(%s, %s, %d, %s, %d)",
                             this.key, this.cmpkey, this.ts,
                             this.event, this.num);
    }
}