package com.ym123.userprofile.etls;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

/**
 * 热词提取（词云）
 * 所谓词云，其实就是统计关键词的频率。
 * 只要是用户评价、商品类别之类的文本信息，我们都可以按空格分词，然后统计每个词出现的次数——就相当于是一个word count，然后按照count数量降序排列就可以了。
 *
 * @author yomo
 * @create 2021-05-10 15:25
 */
public class HortWordETL {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("hot word etl").setMaster("local[*]");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        //数据文件在hdfs上
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        JavaRDD<String> stringJavaRDD = jsc.textFile("");//读取hdfs上的数据

        //mapToPair 得到二元组 准备wordCount
        JavaPairRDD<String, Integer> pairRDD = stringJavaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                //以制表符分隔 取第三个字段
                String word = s.split("\t")[2];
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        //以word 作为key ，分组聚合
        JavaPairRDD<String, Integer> countRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // 元素互换位置
        JavaPairRDD<Integer, String> swapRDD = countRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.swap();
            }
        });

        //按count 降序排序  排序只能按第一位排序 所以加了一步互换位置
        JavaPairRDD<Integer, String> sortRDD = swapRDD.sortByKey(false);

        //再互换位置
        JavaPairRDD<String, Integer> resultRDD = sortRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2.swap();
            }
        });

        //取前10个热词输出
        List<Tuple2<String, Integer>> hotWordCounts = resultRDD.take(10);

        //打印输出
        for (Tuple2<String, Integer> hotWordCount : hotWordCounts) {
            System.out.println(hotWordCount._1 + "==count==" + hotWordCount._2);
        }

    }

}
