package com.lxs.flink.utils;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.springframework.util.ResourceUtils;

import java.io.*;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/14
 **/
public class JSUtil {

    // 如果要更换运行环境，请注意exePath最后的phantom.exe需要更改。因为这个只能在window版本上运行。前面的路径名
    // 也需要和exePath里面的保持一致。否则无法调用
    private static String projectPath = System.getProperty("user.dir");

    static {
        try {
            projectPath = ResourceUtils.getFile("classpath:").getPath();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private static String jsPath = projectPath + File.separator + "phantomjs-2.1.1-windows" + File.separator + "bjmemc.js";
    private static String exePath = projectPath + File.separator + "phantomjs-2.1.1-windows" + File.separator + "bin" + File.separator
            + "phantomjs.exe";
    private static final String requestUrl = "http://zx.bjmemc.com.cn/getAqiList.shtml?timestamp=%d";

    public static void main(String[] args) throws IOException {
        System.out.println(projectPath);
        String url = requestUrl;
        url = String.format(url, System.currentTimeMillis());
        // 测试调用。传入url即可
        String html = getParseredHtml2(url);
        System.out.println("html: " + html);
        Document document = Jsoup.parse(html);
        Elements pointEls = document.select("ul#msg_type li.type_name");
        Elements tabEls = document.select("ul#msg_type li.type_jcd");
    }

    /**
     * 调用phantomjs程序，并传入js文件，并通过流拿回需要的数据
     */
    private static String getParseredHtml2(String url) throws IOException {
        Runtime rt = Runtime.getRuntime();
        Process p = rt.exec(exePath + " " + jsPath + " " + url);
        InputStream is = p.getInputStream();
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        StringBuilder sbf = new StringBuilder();
        String tmp;
        while ((tmp = br.readLine()) != null) {
            sbf.append(tmp);
        }
        return sbf.toString();
    }
}
