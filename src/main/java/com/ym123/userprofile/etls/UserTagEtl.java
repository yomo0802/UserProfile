package com.ym123.userprofile.etls;

import com.ym123.userprofile.utils.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import java.io.Serializable;
import java.util.List;

/**
 * 用户标签系统（用户画像建模）
 * <p>
 * 现在电商行业竞争越来越激烈，只有泛泛的数据统计是不够的。
 * 很多大厂都开始向着所谓“精准营销”“个性化推荐”这些领域来发力。
 * 而精准营销的基础，就是要给用户打上各种各样、丰富而且不同的标签。
 * 所以我们接下来做的就是，用sql把用户标签提取出来，而得到的标签结果要写入es，供业务系统读取查询
 *
 * @author yomo
 * @create 2021-05-10 17:35
 */
public class UserTagEtl {
    public static void main(String[] args) {

        SparkSession session = SparkUtils.initSession();
        //定义一个方法，将链表查询的结果存到es
        etl(session);
    }

    // 提取用户标签
    private static void etl(SparkSession session) {
        // 提取用户基本信息标签
        Dataset<Row> memberBase = session.sql(
                "select id as memberId,phone, sex,member_channel as channel, mp_open_id as subOpenId," +
                        " address_default_id as address, date_format(create_time,'yyyy-MM-dd') as regTime" +
                        " from ecommerce.t_member");

        // 提取用户购买行为特征
        Dataset<Row> orderBehavior = session.sql(
                "select o.member_id as memberId," +
                        " count(o.order_id) as orderCount," +
                        " date_format(max(o.create_time),'yyyy-MM-dd') as orderTime," +
                        " sum(o.pay_price) as orderMoney, " +
                        " collect_list(DISTINCT oc.commodity_id) as favGoods " +
                        " from ecommerce.t_order as o left join ecommerce.t_order_commodity as oc" +
                        " on o.order_id = oc.order_id group by o.member_id");

        // 提取用户购买能力标签
        Dataset<Row> freeCoupon = session.sql(
                "select member_id as memberId, " +
                        " date_format(create_time,'yyyy-MM-dd') as freeCouponTime " +
                        " from ecommerce.t_coupon_member where coupon_id = 1");

        // 多次购买购物券的时间
        Dataset<Row> couponTimes = session.sql(
                "select member_id as memberId," +
                        " collect_list(date_format(create_time,'yyyy-MM-dd')) as couponTimes" +
                        "  from ecommerce.t_coupon_member where coupon_id !=1 group by member_id");

        // 买购物券总的花费金额
        Dataset<Row> chargeMoney = session.sql(
                "select cm.member_id as memberId , sum(c.coupon_price/2) as chargeMoney " +
                        " from ecommerce.t_coupon_member as cm left join ecommerce.t_coupon as c " +
                        " on cm.coupon_id = c.id where cm.coupon_channel != 1 group by cm.member_id");

        // 用户对服务的反馈行为特征
        Dataset<Row> overTime = session.sql(
                "select (to_unix_timestamp(max(arrive_time)) - to_unix_timestamp(max(pick_time))) " +
                        " as overTime, member_id as memberId " +
                        " from ecommerce.t_delivery group by member_id");

        // 最近一次用户反馈
        Dataset<Row> feedback = session.sql("select fb.feedback_type as feedback,fb.member_id as memberId" +
                " from ecommerce.t_feedback as fb " +
                " right join (select max(id) as fid,member_id as memberId " +
                " from ecommerce.t_feedback group by member_id) as t " +
                " on fb.id = t.fid");

        // 将这些结果注册成表，全部按照id连接起来，合并出想要的信息，写入es中
        memberBase.registerTempTable("memberBase");
        orderBehavior.registerTempTable("orderBehavior");
        freeCoupon.registerTempTable("freeCoupon");
        couponTimes.registerTempTable("couponTimes");
        chargeMoney.registerTempTable("chargeMoney");
        overTime.registerTempTable("overTime");
        feedback.registerTempTable("feedback");

        Dataset<Row> result = session.sql("select m.*,o.orderCount,o.orderTime,o.orderMoney,o.favGoods," +
                " fb.freeCouponTime,ct.couponTimes, cm.chargeMoney,ot.overTime,f.feedBack" +
                " from memberBase as m " +
                " left join orderBehavior as o on m.memberId = o.memberId " +
                " left join freeCoupon as fb on m.memberId = fb.memberId " +
                " left join couponTimes as ct on m.memberId = ct.memberId " +
                " left join chargeMoney as cm on m.memberId = cm.memberId " +
                " left join overTime as ot on m.memberId = ot.memberId " +
                " left join feedback as f on m.memberId = f.memberId ");

        JavaEsSparkSQL.saveToEs(result, "/usertag/_doc");
    }


    // 定义用户标签的VO
    @Data
    public static class MemberTag implements Serializable {
        // 用户基本信息
        private String memberId;
        private String phone;
        private String sex;
        private String channel;
        private String subOpenId;
        private String address;
        private String regTime;

        // 用户业务行为特征
        private Long orderCount;
        private String orderTime;
        private Double orderMoney;
        private List<String> favGoods;

        // 用户购买能力
        private String freeCouponTime;   // 首单免费时间
        private List<String> couponTimes;    // 多次购买时间
        private Double chargeMoney;    // 购买花费金额

        // 用户反馈行为特征
        private Integer overTime;
        private Integer feedBack;


    }
}
