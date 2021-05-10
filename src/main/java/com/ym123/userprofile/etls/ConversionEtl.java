package com.ym123.userprofile.etls;

import com.ym123.userprofile.utils.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 用户行为转化率分析（漏斗图）
 * <p>
 * 在一个电商平台上，用户可以有不同的业务行为：
 * 页面展现 -> 点击 -> 加购物车 -> 下单 -> 复购 -> 充值（购买优惠券）
 * 对于用户的一些行为，我们是希望做沉降分析的，考察用户每一步行为的转化率。这就可以画出一个“漏斗图”
 *
 * @author yomo
 * @create 2021-05-10 17:31
 */
public class ConversionEtl {

    public static void main(String[] args) {

        SparkSession session = SparkUtils.initSession();

        ConversionVo conversionVo = conversionBehaviorCount(session);
        System.out.println(conversionVo);
    }

    public static ConversionVo conversionBehaviorCount(SparkSession session) {
        // 查询下过订单的用户
        Dataset<Row> orderMember = session.sql("select distinct(member_id) from ecommerce.t_order " +
                "where order_status=2");

        // 将购买次数超过 1 次的用户查出来
        Dataset<Row> orderAgainMember = session.sql("select t.member_id as member_id " +
                " from (select count(order_id) as orderCount," +
                " member_id from ecommerce.t_order " +
                " where order_status=2 group by member_id) as t " +
                " where t.orderCount>1");

        // 查询充值过的用户
        Dataset<Row> charge = session.sql("select distinct(member_id) as member_id " +
                "from ecommerce.t_coupon_member where coupon_channel = 1");

        Dataset<Row> join = charge.join(
                orderAgainMember,
                orderAgainMember.col("member_id")
                        .equalTo(charge.col("member_id")),
                "inner");

        // 统计各层级的数量
        long order = orderMember.count();
        long orderAgain = orderAgainMember.count();
        long chargeCoupon = join.count();

        // 包装成VO
        ConversionVo vo = new ConversionVo();
        vo.setPresent(1000L); // 目前数据中没有，直接给定值
        vo.setClick(800L);
        vo.setAddCart(600L);
        vo.setOrder(order);
        vo.setOrderAgain(orderAgain);
        vo.setChargeCoupon(chargeCoupon);

        return vo;
    }

    @Data
    static class ConversionVo {
        private Long present;
        private Long click;
        private Long addCart;
        private Long order;
        private Long orderAgain;
        private Long chargeCoupon;


    }

}
