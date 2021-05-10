package com.ym123.userprofile.app.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ym123.userprofile.app.service.EsQueryService;
import com.ym123.userprofile.app.support.EsQueryTag;
import com.ym123.userprofile.etls.UserTagEtl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;

/**
 * 响应es查询请求的控制器（EsQueryController）
 * 用来处理es相关操作的请求。定义的请求路径为/gen，
 * 这应该是一个POST请求，用来下载圈好的用户和手机号，页面点击“生成报告”，就可以直接从es生成报告。
 *
 * @author yomo
 * @create 2021-05-10 21:29
 */
public class EsQueryController {

    @Autowired
    EsQueryService service;

    // 需要提交表单，POST请求
    @RequestMapping("/gen")
    public void genAndDown(HttpServletResponse response, @RequestBody String data) {
        JSONObject object = JSON.parseObject(data);
        JSONArray selectedTags = object.getJSONArray("selectedTags");
        List<EsQueryTag> list = selectedTags.toJavaList(EsQueryTag.class);

// 调用服务，按照查询限制提取出对应的用户信息
        List<UserTagEtl.MemberTag> tags = service.buildQuery(list);

// 自定义方法，将所有用户信息拼在一起写入文件
        String content = toContent(tags);
        String fileName = "member.txt";

        response.setContentType("application/octet-stream");
        try {
            response.setHeader("Content-Disposition", "attachment; filename=" + URLEncoder.encode(fileName, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        try {
            ServletOutputStream sos = response.getOutputStream();
            BufferedOutputStream bos = new BufferedOutputStream(sos);
            bos.write(content.getBytes("UTF-8"));
            bos.flush();    // 直接将缓冲池中数据刷出，默认是要填满才发
            bos.close();
            sos.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String toContent(List<UserTagEtl.MemberTag> tags) {
        StringBuilder sb = new StringBuilder();
        for (UserTagEtl.MemberTag tag : tags) {
            sb.append("[" + tag.getMemberId() + "," + tag.getPhone() + "]\r\n");
        }
        return sb.toString();
    }


}
