package com.ym123.userprofile.utils;

import org.apache.spark.sql.SparkSession;

/**
 * @author yomo
 * @create 2021-05-10 10:26
 */
public class SparkUtils {

    // 定义spark session会话池
    private static ThreadLocal<SparkSession> sessionPool = new ThreadLocal<>();

    //初始化spark session 会话池
    public static SparkSession initSession() {
        //判断会话池中是否有Session 有直接用 没有就创建
        if (sessionPool.get() != null) {
            return sessionPool.get();
        }
        SparkSession session = SparkSession.builder().appName("etl")
                .master("local[*]")
                .config("es.nodes", "hadoop002")
                .config("es.port", "9200")
                .config("es.index.auto.create", "false")
                .enableHiveSupport() //spark on hive 启用hive支持
                .getOrCreate();
        sessionPool.set(session);
        return session;
    }


}
