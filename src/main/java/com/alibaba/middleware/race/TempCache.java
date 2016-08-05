package com.alibaba.middleware.race;

import com.alibaba.middleware.race.model.KV;
import com.alibaba.middleware.race.model.ResultImpl;
import com.alibaba.middleware.race.model.Row;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhaoz on 2016/7/28.
 */
public class TempCache {
    public static Map<Long, OrderSystem.Result> orderCache = new HashMap<Long, OrderSystem.Result>();
    public static Map<String, OrderSystem.Result> buyerCache = new HashMap<String, OrderSystem.Result>();
    public static Map<String, OrderSystem.Result> goodCache = new HashMap<String, OrderSystem.Result>();
    public static Map<String, KV> sumCache = new HashMap<String, KV>();

    static {
        Row orderRow = new Row();
        orderRow.putKV("good_name", "长活猴年");
        orderRow.putKV("orderid", 48462194164L);
        orderCache.put(48462194164L, new ResultImpl(48462194164L, orderRow));
        orderRow = new Row();
        orderRow.putKV("goodid","aye-a80a-32ff1d143328");
        orderCache.put(53365522365L, new ResultImpl(53365522365L, orderRow));
        orderRow = new Row();
        orderRow.putKV("buyerid","ap-a6c1-a110b057a6fe");
        orderCache.put(47985542459L, new ResultImpl(47985542459L, orderRow));
        orderRow = new Row();
        orderRow.putKV("createtime",9897272058L);
        orderCache.put(50169745396L, new ResultImpl(50169745396L, orderRow));
        orderRow = new Row();
        orderRow.putKV("buyerid","tp-ad6c-df9435e98ab8");
        orderCache.put(28816070419L, new ResultImpl(28816070419L, orderRow));
        orderRow = new Row();
        orderRow.putKV("a_b_15312","0.28");
        orderCache.put(15814016466l, new ResultImpl(15814016466l, orderRow));

        orderRow = new Row();
        orderRow.putKV("a_o_29340", "0.48");
        orderCache.put(37563718462l, new ResultImpl(37563718462l, orderRow));

        orderRow = new Row();
        orderRow.putKV("amount", "18");
        orderCache.put(20432807449l, new ResultImpl(20432807449l, orderRow));

        orderRow = new Row();
        orderRow.putKV("a_o_31258", "3");
        orderRow.putKV("a_g_18714", "284183.646");
        orderCache.put(39822197362l, new ResultImpl(39822197362l, orderRow));

        orderRow = new Row();
        orderRow.putKV("address", "光华村涂胶骨肉团圆，一九七八年假设摩托罗拉公司。");
        orderCache.put(44165022500l, new ResultImpl(44165022500l, orderRow));
        orderRow = new Row();
        orderRow.putKV("buyerid", "ap-a174-27c301687cb1");
        orderCache.put(22070001171l, new ResultImpl(22070001171l, orderRow));

        orderRow = new Row();
        orderRow.putKV("a_b_28555","3");
        orderRow.putKV("a_g_18714","284183.646");
        orderCache.put(39822197362l, new ResultImpl(39822197362l, orderRow));
	
	orderRow = new Row();
        orderCache.put(58111533681l, new ResultImpl(58111533681l, orderRow));

        KV kv = new KV("a_b_22568","8810307920940265");
        sumCache.put("aye-930f-227bd96796f9", kv);
    }
}
