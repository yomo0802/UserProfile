package com.ym123.userprofile.etls;

import com.alibaba.fastjson.JSON;
import com.ym123.userprofile.dateutils.DateStyle;
import com.ym123.userprofile.dateutils.DateUtil;
import com.ym123.userprofile.utils.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 营销提醒统计（饼图）
 * <p>
 * 营销提醒，主要是优惠券过期提醒，主要分两类：
 * 首单免费优惠券（coupon_id为1），以及一般的抵扣优惠券（代金券，coupon_id不为1）。
 * 我们可以将所有优惠券，按照失效时间统计出来，以便考虑优惠券的使用情况，快速调整营销策略。
 *
 * @author yomo
 * @create 2021-05-10 17:26
 */
public class RemindEtl {

    public static void main(String[] args) {

        SparkSession session = SparkUtils.initSession();

        // 查询近一周优惠券的数量
        List<FreeRemindVo> freeVos = freeRemindCount(session);
        List<CouponRemindVo> couponVos = couponRemindCount(session);

        System.out.println("======" + freeVos);
        System.out.println("======" + couponVos);
    }

    public static List<FreeRemindVo> freeRemindCount(SparkSession session) {
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);
        Date nowDay = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());
        Date sevenDayBefore = DateUtil.addDay(nowDay, -7);

        String sql = "select date_format(create_time,'yyyy-MM-dd') as day, " +
                " count(member_id) as freeCount " +
                " from ecommerce.t_coupon_member where coupon_id = 1 " +
                " and coupon_channel = 2 and create_time >= '%s' " +
                " group by date_format(create_time,'yyyy-MM-dd')";

        sql = String.format(sql,
                DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> dataset = session.sql(sql);

        List<String> list = dataset.toJSON().collectAsList();
        List<FreeRemindVo> result = list.stream()
                .map(str -> JSON.parseObject(str, FreeRemindVo.class))
                .collect(Collectors.toList());
        return result;

    }

    public static List<CouponRemindVo> couponRemindCount(SparkSession session) {
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);
        Date nowDay = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());
        Date sevenDayBefore = DateUtil.addDay(nowDay, -7);

        String sql = "select date_format(create_time,'yyyy-MM-dd') as day, " +
                " count(member_id) as couponCount " +
                " from ecommerce.t_coupon_member where coupon_id != 1 " +
                " and create_time >= '%s' " +
                " group by date_format(create_time,'yyyy-MM-dd')";

        sql = String.format(sql,
                DateUtil.DateToString(sevenDayBefore, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> dataset = session.sql(sql);

        List<String> list = dataset.toJSON().collectAsList();
        List<CouponRemindVo> result = list.stream()
                .map(str -> JSON.parseObject(str, CouponRemindVo.class))
                .collect(Collectors.toList());
        return result;

    }

    @Data
    static class FreeRemindVo {
        private String day;
        private Integer freeCount;
    }

    @Data
    static class CouponRemindVo {
        private String day;
        private Integer couponCount;
    }

}

