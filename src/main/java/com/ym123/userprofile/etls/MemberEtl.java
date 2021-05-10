package com.ym123.userprofile.etls;

import com.alibaba.fastjson.JSON;
import com.ym123.userprofile.utils.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 用户信息提取
 * <p>
 * 我们关心的用户信息的分布情况，主要有性别、渠道、是否订阅（关注公众号）、热度。
 * 这些数据，应该就是：当前有多少男性用户、多少女性用户、多少安卓用户、多少ios用户…全部都是统计一个count值。
 * 最终，我们应该把这些数据包在一起，供前端页面来读取，就可以画出上面的饼图了。
 *
 * @author yomo
 * @create 2021-05-10 10:30
 */
public class MemberEtl {

    public static void main(String[] args) {

        //1.初始化sparkSession
        SparkSession sparkSession = SparkUtils.initSession();

        //2.写sql查询想要的数据
        List<MemberSex> memberSexes = memberSexEtl(sparkSession);
        List<MemberChannel> memberChannels = memberChannelEtl(sparkSession);
        List<MemberMpSub> memberMpSubs = memberMpSubEtl(sparkSession);
        MemberHeat memberHeats = memberHeatEtl(sparkSession);

        //3.数据展示结果，提供前端页面调用
        MemberVo memberVo = new MemberVo();
        memberVo.setMemberSexes(memberSexes);
        memberVo.setMemberChannels(memberChannels);
        memberVo.setMemberMpSubs(memberMpSubs);
        memberVo.setMemberHeats(memberHeats);

        //测试打印
        System.out.println(JSON.toJSONString(memberVo));

    }

    //性别统计信息
    public static List<MemberSex> memberSexEtl(SparkSession session) {
        //先用sql得到每个性别的count统计数据
        Dataset<Row> dataset = session.sql(
                "select " +
                        "   sex as memberSex," +
                        "   count(id) as sexCount " +
                        "from" +
                        "   ecommerce.t_member " +
                        "group by " +
                        "   sex");
        //将dataSet 转换为List
        List<String> list = dataset.toJSON().collectAsList();

        //将List转换为流,对每一个元素依次遍历 转换为MemberSex
        List<MemberSex> result = list.stream()
                .map(str -> JSON.parseObject(str, MemberSex.class))
                .collect(Collectors.toList());

        return result;
    }

    //渠道来源信息
    public static List<MemberChannel> memberChannelEtl(SparkSession session) {
        Dataset<Row> dataset = session.sql(
                "select" +
                        "   member_channel as memberChannel," +
                        "   count(id) as channelCount " +
                        "from " +
                        "   ecommerce.t_member " +
                        "group by member_channel"
        );

        List<String> list = dataset.toJSON().collectAsList();

        List<MemberChannel> result = list.stream()
                .map(str -> JSON.parseObject(str, MemberChannel.class))
                .collect(Collectors.toList());

        return result;
    }

    //用户是否关注媒体平台
    public static List<MemberMpSub> memberMpSubEtl(SparkSession session) {
        Dataset<Row> dataset = session.sql("" +
                "select" +
                "   count(if(mp_open_id != 'null' , true ,null)) as subCount," +
                "   count(if(mp_open_id = 'null' ,true , null)) as unsubCount" +
                "from " +
                "   ecommerce.t_member");

        List<String> list = dataset.toJSON().collectAsList();

        List<MemberMpSub> result = list.stream()
                .map(str -> JSON.parseObject(str, MemberMpSub.class))
                .collect(Collectors.toList());

        return result;
    }

    //用户热度统计
    public static MemberHeat memberHeatEtl(SparkSession session) {
        //注册
        Dataset<Row> reg_complete = session.sql(
                "select" +
                        "   count(if(phone ='null',true,null)) as reg " +
                        "   count(if(phone != 'null' ,true ,null) as complete) " +
                        "from " +
                        "   ecommerce.t_member");
        //复购
        Dataset<Row> order_again = session.sql(
                "select " +
                        "   count(if(t.orderCount =1 ,true ,null)) as order ," +
                        "   count(if(t.orderCount >= 2 ,true , null )) as orderAgain" +
                        "from " +
                        "   (select" +
                        "       count(order_id) as orderCount," +
                        "       member_id " +
                        "   from" +
                        "       ecommerce.t.order" +
                        "   group by member_id) as t");
        //优惠劵
        Dataset<Row> coupon = session.sql(
                "select" +
                        "   count(distinct member_id) as coupon " +
                        "from " +
                        "ecommerce.t_coupon_member ");
        //将三张表(注册、复购、优惠劵)连在一起
        Dataset<Row> heat = coupon.crossJoin(reg_complete).crossJoin(order_again);

        List<String> list = heat.toJSON().collectAsList();
        List<MemberHeat> result = list.stream()
                .map(str -> JSON.parseObject(str, MemberHeat.class))
                .collect(Collectors.toList());
        //只有一行数据 获取后返回
        return result.get(0);

    }


    //定义一个最终想要生成的VO,用来展示饼图的数据信息
    @Data
    static class MemberVo {
        //由4部分组成
        private List<MemberSex> memberSexes; //性别统计信息
        private List<MemberChannel> MemberChannels; //渠道来源信息
        private List<MemberMpSub> MemberMpSubs; //用户是否关注媒体平台
        private MemberHeat MemberHeats; //用户热度统计
    }

    @Data
    static class MemberSex {
        private Integer memberSex;//性别编号
        private Integer sexCount;//性别数量
    }

    @Data
    static class MemberChannel {
        private Integer memberChannel;
        private Integer channelCount;
    }

    @Data
    static class MemberMpSub {
        private Integer subCount;
        private Integer unSubCount;
    }

    @Data
    static class MemberHeat {
        private Integer reg;    // 只注册，未填写手机号
        private Integer complete;    // 完善了信息，填了手机号
        private Integer order;    // 下过订单
        private Integer orderAgain;    // 多次下单，复购
        private Integer coupon;    // 购买过优惠券，储值
    }
}
