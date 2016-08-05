package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.OrderSystem;

import java.util.Set;

/**
 * Created by zhaoz on 2016/7/28.
 */
public class ResultImpl implements OrderSystem.Result, Comparable {
    private long orderid;
    private Row kvMap;

    public ResultImpl(long orderid, Row kv) {
        this.orderid = orderid;
        this.kvMap = kv;
    }

    public static ResultImpl createResultRow(Row orderData, Row buyerData,
                                             Row goodData, Set<String> queryingKeys) {
        if (orderData == null) {
            throw new RuntimeException("Bad data!");
        }
        Row allkv = new Row();
        long orderid;
        try {
            orderid = orderData.get("orderid").valueAsLong();
        } catch (OrderSystem.TypeException e) {
            throw new RuntimeException("Bad data!");
        }

        for (KV kv : orderData.values()) {
            if (queryingKeys == null || queryingKeys.contains(kv.key)) {
                allkv.put(kv.key(), kv);
            }
        }

        if (buyerData != null)
            for (KV kv : buyerData.values()) {
                if (queryingKeys == null || queryingKeys.contains(kv.key)) {
                    allkv.put(kv.key(), kv);
                }
            }
        if (goodData != null)
            for (KV kv : goodData.values()) {
                if (queryingKeys == null || queryingKeys.contains(kv.key)) {
                    allkv.put(kv.key(), kv);
                }
            }
        return new ResultImpl(orderid, allkv);
    }

    public OrderSystem.KeyValue get(String key) {
        return this.kvMap.get(key);
    }

    public OrderSystem.KeyValue[] getAll() {
        return kvMap.values().toArray(new OrderSystem.KeyValue[0]);
    }

    public long orderId() {
        return orderid;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("orderid: " + orderid + " {");
        sb.append(kvMap);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int compareTo(Object o) {
        return (int) (orderid - ((ResultImpl) o).orderId());
    }
}