package com.lxs.flink.utils;

import com.gargoylesoftware.htmlunit.*;
import com.gargoylesoftware.htmlunit.util.Cookie;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class WebClintUtils {

    private static final Logger logger = LoggerFactory.getLogger(WebClintUtils.class);

    /**
     * 初始化HtmlUnit对象
     * 启用JS解释器，默认为true
     * 禁用css支持
     * js运行错误时，是否抛出异常
     * 设置支持ajax
     * 接受任何主机连接，不管证书是否有效
     * 运行重定向
     */
    public static WebClient initWebClient() {
        WebClient webClient = new WebClient();
        webClient.getOptions().setJavaScriptEnabled(true);
        webClient.getOptions().setCssEnabled(false);
        webClient.getOptions().setThrowExceptionOnScriptError(false);
        webClient.getOptions().setThrowExceptionOnFailingStatusCode(false);
        webClient.getOptions().setActiveXNative(false);
        webClient.setAjaxController(new NicelyResynchronizingAjaxController());
        webClient.getOptions().setUseInsecureSSL(true);
        webClient.getOptions().setTimeout(20000);
        webClient.getOptions().setRedirectEnabled(true);
        webClient.setJavaScriptTimeout(50000);
        //webClient.addRequestHeader("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36");
        String user_agent = UserAgentUtils.getRandomUserAgent();
        webClient.addRequestHeader("User-Agent", user_agent);
        return webClient;
    }

    /**
     * 执行请求，获取内容
     */
    public static String getWebContent(WebClient webClient, String url, int count, int maxCount) throws Exception {
        if (count > maxCount) {
            throw new Exception(maxCount + "次重试请求页面失败！");
        }
        int changeProxyTime = 3;
        if (count == changeProxyTime) {
            // TODO 修改代理、测试当前代理是否正常
        }
        String content = "";
        try {
            Page page = webClient.getPage(url);
            webClient.waitForBackgroundJavaScript(20000);
            WebResponse webResponse = page.getWebResponse();
            content = webResponse.getContentAsString();
        } catch (IOException e) {
            logger.error(String.format("访问[%s]出错,次数[%s]", url, count), e);
            TimeUnit.SECONDS.sleep(1);
            getWebContent(webClient, url, count + 1, maxCount);
        }
        return content;
    }

    public static WebClient getWebClient(WebClient webClient, Map<String, String> requestHeaderMap, String url, String refer) {
        if (webClient == null) {
            webClient = initWebClient();
        }
        changeableWebClient(webClient, requestHeaderMap, url, refer);
        return webClient;
    }

    private static void changeableWebClient(WebClient webClient, Map<String, String> requestHeaderMap, String url, String refer) {
        try {
            String host = new URL(url).getHost();
            webClient.addRequestHeader("Host", host);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        if (StringUtils.isNotBlank(refer)) {
            webClient.addRequestHeader("Referer", refer);
        }

        logger.info("处理url：" + url);

        if (!CollectionUtils.isEmpty(requestHeaderMap)) {
            requestHeaderMap.forEach((key, value) -> {
                if ("cookie".equals(key.toLowerCase()) && !StringUtils.isEmpty(value)) {
                    value = value.replaceAll("=", ":");
                    String[] cookies = value.split(";");
                    Map<String, String> cookieMap = new HashMap<>(cookies.length);
                    Arrays.stream(cookies).forEach(str -> {
                        String[] cookieStr = str.split(":");
                        cookieMap.put(cookieStr[0], cookieStr[1]);
                    });
                    if (!CollectionUtils.isEmpty(cookieMap)) {
                        CookieManager cookieManager = webClient.getCookieManager();
                        cookieManager.setCookiesEnabled(true);
                        cookieMap.forEach((cookie_key, cookie_value) -> {
                            Cookie cookie = new Cookie("zx.bjmemc.com.cn", cookie_key, cookie_value);
                            cookieManager.addCookie(cookie);
                        });
                    }
                } else {
                    webClient.addRequestHeader(key, value);
                }
            });
        }
    }

    public static void configProxy(WebClient client, String host, Integer port, String userName, String password) {
        ProxyConfig proxyConfig = client.getOptions().getProxyConfig();
        proxyConfig.setProxyHost(host);
        proxyConfig.setProxyPort(port);
        if (StringUtils.isNotBlank(userName) && StringUtils.isNotBlank(password)) {
            DefaultCredentialsProvider provider = (DefaultCredentialsProvider) client.getCredentialsProvider();
            provider.addCredentials(userName, password);
        }
    }
}
