package com.lxs.flink.utils;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.openqa.selenium.By;
import org.openqa.selenium.Proxy;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.remote.CapabilityType;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.concurrent.TimeUnit;

/**
 * @Description
 * @Author hasee
 * @Date 2018/12/14
 **/
public class PhantomJs {
    private static final Logger logger = LoggerFactory.getLogger(PhantomJs.class);
    private static final String requestUrl = "http://zx.bjmemc.com.cn/getAqiList.shtml?timestamp=%d";
    // 如果要更换运行环境，请注意exePath最后的phantom.exe需要更改。因为这个只能在window版本上运行。前面的路径名
    // 也需要和exePath里面的保持一致。否则无法调用
    private static String projectPath = System.getProperty("user.dir");

    static {
        try {
            projectPath = ResourceUtils.getFile("classpath:").getPath();
        } catch (FileNotFoundException e) {
            logger.error("获取resource下面的爬虫工具phantomjs相关文件出错");
            System.exit(1);
        }
    }

    private static String jsPath = projectPath + File.separator + "phantomjs-2.1.1-windows" + File.separator + "bjmemc.js";
    private static String winExePath = projectPath + File.separator + "phantomjs-2.1.1-windows" + File.separator + "bin" + File.separator + "phantomjs.exe";
    private static String linuxExePath = projectPath + File.separator + "phantomjs-2.1.1-windows" + File.separator + "bin" + File.separator + "phantomjs";

    public static WebDriver getPhantomJs() {
        String osname = System.getProperties().getProperty("os.name");
        //判断系统的环境win or Linux
        if (osname.equals("Linux")) {
            System.setProperty("phantomjs.binary.path", linuxExePath);
        } else {
            System.setProperty("phantomjs.binary.path", winExePath);//设置PhantomJs访问路径
        }
        DesiredCapabilities desiredCapabilities = DesiredCapabilities.phantomjs();
        //设置参数
        desiredCapabilities.setCapability("phantomjs.page.settings.userAgent", "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:50.0) Gecko/20100101 Firefox/50.0");
        desiredCapabilities.setCapability("phantomjs.page.customHeaders.User-Agent", "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:50.0) Gecko/20100101 　　Firefox/50.0");
        //是否使用代理
        Proxy proxy = new Proxy();
        proxy.setProxyType(Proxy.ProxyType.MANUAL);
        proxy.setAutodetect(false);
        String proxyStr;
        do {
            proxyStr = "192.168.20.24:808";
        } while (proxyStr.length() == 0);
        proxy.setHttpProxy(proxyStr);
        desiredCapabilities.setCapability(CapabilityType.PROXY, proxy);
        return new PhantomJSDriver(desiredCapabilities);
    }

    public static void main(String[] args) {
        WebDriver webDriver = null;
        try {
            String url = requestUrl;
            url = String.format(url, System.currentTimeMillis());
            webDriver = PhantomJs.getPhantomJs();
            webDriver.manage().timeouts().implicitlyWait(10,TimeUnit.SECONDS);
            webDriver.get(url);
            webDriver.manage().timeouts().pageLoadTimeout(10,TimeUnit.SECONDS);
            WebDriverWait wait = new WebDriverWait(webDriver, 10);
            wait.until(ExpectedConditions.presenceOfElementLocated(By.xpath("/html")));
            Document document = Jsoup.parse(webDriver.getPageSource());
            //TODO 　剩下页面的获取就按照Jsoup获取方式来做
            Elements pointEls = document.select("ul#msg_type li.type_name");
            Elements tabEls = document.select("ul#msg_type li.type_jcd");
            System.out.println(tabEls);
        } finally {
            if (webDriver != null) {
                webDriver.quit();
            }
        }
    }
}
