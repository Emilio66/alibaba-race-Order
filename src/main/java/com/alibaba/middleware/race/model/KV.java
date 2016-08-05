package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.OrderSystem;

public class KV implements Comparable<KV>, OrderSystem.KeyValue {

        String key;
        public String rawValue;

        boolean isComparableLong = false;
        public long longValue;

        public KV(String key, String rawValue) {
            this.key = key;
            this.rawValue = rawValue.trim();
            if (key.equals("createtime") || key.equals("orderid")) {
                isComparableLong = true;
                longValue = Long.parseLong(rawValue);
            }
        }

        public String key() {
            return key;
        }

        public String valueAsString() {
            return rawValue;
        }

        public long valueAsLong() throws OrderSystem.TypeException {
            try {
                return Long.parseLong(rawValue);
            } catch (NumberFormatException e) {
                System.out.println("--Type Error : key " + key + ", raw value: " + rawValue + ", is long: " + isComparableLong
                        + " , Error: " + e);
                throw new OrderSystem.TypeException();
            }
        }

        public double valueAsDouble() throws OrderSystem.TypeException {
            try {
                return Double.parseDouble(rawValue);
            } catch (NumberFormatException e) {
                System.out.println("--Type Error : key " + key + ", raw value: " + rawValue + ", is long: " + isComparableLong
                        + " , Error: " + e);
                throw new OrderSystem.TypeException();
            }
        }

        public boolean valueAsBoolean() throws OrderSystem.TypeException {
            if (this.rawValue.equals("true")) {
                return true;
            }
            if (this.rawValue.equals("false")) {
                return false;
            }
            throw new OrderSystem.TypeException();
        }

        public int compareTo(KV o) {
            if (!this.key().equals(o.key())) {
                throw new RuntimeException("Cannot compare from different key");
            }
            if (isComparableLong) {
                return Long.compare(this.longValue, o.longValue);
            }
            return this.rawValue.compareTo(o.rawValue);
        }

        @Override
        public String toString() {
            return "[" + this.key + "]:" + this.rawValue;
        }
    }