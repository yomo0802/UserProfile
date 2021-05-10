package com.ym123.userprofile.app.support;

import lombok.Data;

/**
 * es查询标签操作数据的类（EsQueryTag）
 * 这是一个定义好的类，专门用于定义es查询操作的字段，也就是这对标签的查询条件，包括name、value和type。
 *
 * @author yomo
 * @create 2021-05-10 21:23
 */
@Data
public class EsQueryTag {
    private String name;    // 标签名称
    private String value;    // 查询限定的值
    private String type;    // 查询类型
}
