package com.ym123.userprofile.app.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**页面控制器
 * @author yomo
 * @create 2021-05-10 21:27
 */
@Controller
public class PageController {

    // 首先是对根路径的处理，加载index.html页面
    @RequestMapping("/")
    public String index() {
        return "index";
    }
    // 对于点选标签圈人的后台管理功能，另设一个地址/tags，加载tags.html
    @RequestMapping("/tags")
    public String tags(){
        return "tags";
    }


}
