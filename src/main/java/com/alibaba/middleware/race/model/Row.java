package com.alibaba.middleware.race.model;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("serial")
    public class Row extends HashMap<String, KV> {
        public Row() {
            super();
        }

        Row(KV kv) {
            super();
            this.put(kv.key(), kv);
        }

        public KV getKV(String key) {
            KV kv = this.get(key);
            if (kv == null) {
                throw new RuntimeException(key + " is not exist");
            }
            return kv;
        }

        public Row putKV(String key, String value) {
            KV kv = new KV(key, value);
            this.put(kv.key(), kv);
            return this;
        }

        public Row putKV(String key, long value) {
            KV kv = new KV(key, Long.toString(value));
            this.put(kv.key(), kv);
            return this;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry entry : this.entrySet())
                builder.append(entry.getValue());
            return builder.toString();
        }
    }
