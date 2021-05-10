package com.ym123.userprofile.etls;

import com.alibaba.fastjson.JSONObject;
import com.ym123.userprofile.dateutils.DateStyle;
import com.ym123.userprofile.dateutils.DateUtil;
import com.ym123.userprofile.utils.SparkUtils;
import lombok.Data;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * 平台近期数据增量（折线图）
 * 我们还可以统计平台用户数据的增长，主要就是注册量和订单量。
 * 我们定义几个要展示的统计指标：（只统计展示近七天的数据）
 * 	近七天的、每天新增注册人数（每天增量）；
 * 	近七天的、截至每天的总用户人数（每天总量）；
 * 	近七天的、截至每天的总订单数（每天总量）；
 * 	近七天的、截至每天的总订单流水金额数量（每天总量）
 *
 * @author yomo
 * @create 2021-05-10 15:48
 */
public class GrowthEtl {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkUtils.initSession();
        List<GrowthLineVo> growthLineVoList = growthEtl(sparkSession);
        //测试打印
        System.out.println(growthLineVoList);
    }

    private static List<GrowthLineVo> growthEtl(SparkSession session) {

        //指定“当前日期”是2021.05.10 这是数据决定的 当前系统时间
        LocalDate now = LocalDate.of(2021, Month.MAY, 10);
        java.util.Date nowDate = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());
        java.util.Date sevenDayBefore = DateUtil.addDay(nowDate, -7);

        // 近七天每天的注册人数统计(新增和总量)
        String memberSql = "select " +
                "date_format(create_time,'yyyy-MM-dd') as day, " +
                "count(id) as regCount, " +
                "max(id) as memberCount " +
                "from " +
                "ecommerce.t_member " +
                "where " +
                "create_time >= '%s' " +
                "group by date_format(create_time,'yyyy-MM-dd') " +
                "order by day";

        memberSql = String.format(memberSql, DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> memberDS = session.sql(memberSql);

        // 近七天每天的订单和流水统计
        String orderSql = "select " +
                "date_format(create_time,'yyyy-MM-dd') as day, " +
                "max(order_id) orderCount, " +
                "sum(origin_price) as gmv " +
                "from ecommerce.t_order " +
                "where create_time >= '%s' " +
                "group by date_format(create_time,'yyyy-MM-dd') " +
                "order by day";
        orderSql = String.format(orderSql, DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> orderDS = session.sql(orderSql);

        //连接查询，按day 内连接
        Dataset<Tuple2<Row, Row>> tuple2Dataset = memberDS.joinWith(orderDS, memberDS.col("day").equalTo(orderDS.col("day")), "inner");

        List<Tuple2<Row, Row>> tuple2s = tuple2Dataset.collectAsList();

        ArrayList<GrowthLineVo> vos = new ArrayList<>();

        //遍历二元组List，包装GroWthLineVo
        for (Tuple2<Row, Row> tuple2 : tuple2s) {
            Row row1 = tuple2._1(); //memberSql结果
            Row row2 = tuple2._2(); //orderSql结果

            JSONObject obj = new JSONObject();

            //提取Row类型里的所有字段
            StructType schema = row1.schema();
            String[] strings = schema.fieldNames();
            for (String string : strings) {
                Object as = row1.getAs(string);
                obj.put(string, as);
            }

            schema = row2.schema();
            strings = schema.fieldNames();
            for (String string : strings) {
                Object as = row2.getAs(string);
                obj.put(string, as);
            }

            GrowthLineVo growthLineVo = obj.toJavaObject(GrowthLineVo.class);
            vos.add(growthLineVo);
        }

        //优化 :七天前,再之前的订单流水总和(GMV)
        String preGmvSql = "select sum(origin_price) as totalGmv " +
                "from ecommerce.t_order" +
                "where create_time < '%s' ";

        preGmvSql = String.format(preGmvSql, DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> gmvDS = session.sql(preGmvSql);

        double previousGmv = gmvDS.collectAsList().get(0).getDouble(0);
        BigDecimal preGmv = BigDecimal.valueOf(previousGmv);

        //之前每天的增量gmv取出 ，依次叠加，得到总和
        //遍历之前得到的每天数据，在preGmv的基础上叠加，得到gmv的总量
        ArrayList<BigDecimal> totalGmvList = new ArrayList<>();
        BigDecimal currentGmv = preGmv;

        for (int i = 0; i < vos.size(); i++) {

            //获取每天的统计数据
/*            GrowthLineVo growthLineVo = vos.get(i);
            currentGmv = currentGmv.add(growthLineVo.getGmv());//加上当前day的gmv新增量
            growthLineVo.setGmv(currentGmv);*/

            GrowthLineVo growthLineVo = vos.get(i);
            BigDecimal gmv = growthLineVo.getGmv(); //当前day的gmv

            BigDecimal temp = gmv.add(preGmv); //加上 当前day的gmv

            for (int j = 0; j < i; j++) {
                GrowthLineVo prev = vos.get(j);
                temp = temp.add(prev.getGmv());
            }
            totalGmvList.add(temp);
        }

        //遍历总量gmv的List，更新vos里面gmv的值
        for (int i = 0; i < totalGmvList.size(); i++) {
            GrowthLineVo growthLineVo = vos.get(i);
            growthLineVo.setGmv(totalGmvList.get(i));
        }

        return vos;

    }

    @Data
    private static class GrowthLineVo {
        //每天新增注册用户数，总用户数、总订单数、总流水GMV
        private String day;
        private Integer regCount;
        private Integer memberCount;
        private Integer orderCount;
        private BigDecimal gmv;
    }
}
