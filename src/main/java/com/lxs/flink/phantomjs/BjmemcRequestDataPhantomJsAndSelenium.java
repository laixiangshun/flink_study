package com.lxs.flink.phantomjs;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.By;
import org.openqa.selenium.Proxy;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriver;
import org.openqa.selenium.phantomjs.PhantomJSDriverService;
import org.openqa.selenium.remote.CapabilityType;
import org.openqa.selenium.remote.DesiredCapabilities;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 爬取北京空气质量 监测点空气质量指数
 * phantomJs + selenium
 * 支持linux系统，windows系统
 **/
@Component
public class BjmemcRequestDataPhantomJsAndSelenium {
    private static final Logger logger = LoggerFactory.getLogger(BjmemcRequestDataPhantomJsAndSelenium.class);

    private static final String requestUrl = "http://zx.bjmemc.com.cn/getAqiList.shtml?timestamp=%d";
    private static Map<String, String> titleMap = new LinkedHashMap<>();
    private static Map<String, String> pointMap = new LinkedHashMap<>();
    /**
     * 匹配括号及其内容
     */
    private static final String brackPattern = "(\\(|（)[^(\\(|（)]+(\\)|）)";
    private static final String tableName = "air_quality_point";

    static {
        titleMap.put("site", "站点");
        titleMap.put("air_quality", "空气质量");
        titleMap.put("aqi", "AQI");
        titleMap.put("primary_pollutant", "首要污染物");
        titleMap.put("evaluation_point", "评价点");
        titleMap.put("data_time", "数据时间");
    }

    //如果要更换运行环境，请注意exePath最后的phantom.exe需要更改。因为这个只能在window版本上运行。前面的路径名
    // 也需要和exePath里面的保持一致。否则无法调用
    private static String winExePath;
    private static String linuxExePath;
//    private static String driverExePath;
//    private static String chromeExePath;

    @PostConstruct
    public void init() {
        try {
            String osname = System.getProperties().getProperty("os.name");
            boolean isLinux = osname.contains("Linux");
            File phantomjs;
//            File driver;
//            File chrome;
            String tmp = System.getProperty("java.io.tmpdir");
            File path = new File(tmp + File.separator + "phantomJs");
//            File driverPath = new File(tmp + File.separator + "chromeDriver");
//            File chromePath = new File(tmp + File.separator + "chrome");
            if (!path.exists()) {
                path.mkdirs();
            }
//            if (!driverPath.exists()) {
//                driverPath.mkdirs();
//            }
//            if (!chromePath.exists()) {
//                chromePath.mkdirs();
//            }
            String phantomJsPath = "http://192.168.10.123/dataflow/stream/phantomjs";
//            String chromeDriverPath = properties.getChromeDriverPath();
//            String chromeUrlPath = properties.getChromePath();
            if (isLinux) {
                phantomjs = new File(path.getAbsolutePath() + File.separator + "phantomjs");
                linuxExePath = phantomjs.getAbsolutePath();
//                driver = new File(driverPath.getAbsolutePath() + File.separator + "ghostdriver");
//                driverExePath = driver.getAbsolutePath();
//                chrome = new File(chromePath.getAbsolutePath() + File.separator + "chrome");
//                chromeExePath = chrome.getAbsolutePath();
            } else {
                if (!StringUtils.endsWithIgnoreCase(phantomJsPath, "exe")) {
                    throw new RuntimeException("phantomjs执行文件不是windows支持的exe执行文件");
                }
                phantomjs = new File(path.getAbsolutePath() + File.separator + "phantomjs.exe");
                winExePath = phantomjs.getAbsolutePath();
//                if (!StringUtils.endsWithIgnoreCase(chromeDriverPath, "exe")) {
//                    throw new RuntimeException("浏览器驱动文件不是windows支持的exe执行文件");
//                }
//                driver = new File(driverPath.getAbsolutePath() + File.separator + "chromedriver.exe");
//                driverExePath = driver.getAbsolutePath();
//                if (!StringUtils.endsWithIgnoreCase(chromeUrlPath, "exe")) {
//                    throw new RuntimeException("浏览器执行文件不是windows支持的exe执行文件");
//                }
//                chrome = new File(chromePath.getAbsolutePath() + File.separator + "chrome.exe");
//                chromeExePath = chrome.getAbsolutePath();
            }
            if (!phantomjs.exists()) {
                downloadPhantomJs(phantomJsPath, phantomjs);
            }
//            if (!driver.exists()) {
//                downloadChromeDriver(chromeDriverPath, driver);
//            }
//            if (!chrome.exists()) {
//                downloadChrome(chromeUrlPath, chrome);
//            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            System.exit(1);
        }
    }

    private void downloadPhantomJs(String phantomJsPath, File phantomjs) throws IOException {
        try {
            URL url = new URL(phantomJsPath);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoInput(true);
            connection.setDoOutput(true);
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            connection.connect();
            FileOutputStream out = new FileOutputStream(phantomjs);
            IOUtils.copy(connection.getInputStream(), out);
            out.flush();
            out.close();
            connection.disconnect();
            IOUtils.close(connection);
            phantomjs.setExecutable(true);
        } catch (IOException e) {
            logger.error("下载phantomJs文件失败", e);
            throw e;
        }
    }

    private void downloadChromeDriver(String chromeDriverPath, File driver) throws IOException {
        try {
            URL url = new URL(chromeDriverPath);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoInput(true);
            connection.setDoOutput(true);
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            connection.connect();
            FileOutputStream out = new FileOutputStream(driver);
            IOUtils.copy(connection.getInputStream(), out);
            out.flush();
            out.close();
            connection.disconnect();
            IOUtils.close(connection);
            driver.setExecutable(true);
        } catch (IOException e) {
            logger.error("下载chrome driver失败", e);
            throw e;
        }
    }

    private void downloadChrome(String chromePath, File chrome) throws IOException {
        try {
            URL url = new URL(chromePath);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setDoInput(true);
            connection.setDoOutput(true);
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            connection.connect();
            FileOutputStream out = new FileOutputStream(chrome);
            IOUtils.copy(connection.getInputStream(), out);
            out.flush();
            out.close();
            connection.disconnect();
            IOUtils.close(connection);
            chrome.setExecutable(true);
        } catch (IOException e) {
            logger.error("下载chrome浏览器失败", e);
            throw e;
        }
    }

    /**
     * 判断系统的环境win or Linux
     * 设置PhantomJs访问路径
     * 设置代理
     **/
    private WebDriver getPhantomJs() {
        String osname = System.getProperties().getProperty("os.name");
        if (osname.contains("Linux")) {
            System.setProperty("phantomjs.binary.path", linuxExePath);
//            System.setProperty("webdriver.chrome.driver", driverExePath);
//            System.setProperty("webdriver.chrome.bin", chromeExePath);
        } else {
            System.setProperty("phantomjs.binary.path", winExePath);
//            System.setProperty("webdriver.chrome.driver", driverExePath);
//            System.setProperty("webdriver.chrome.bin", chromeExePath);
        }
        DesiredCapabilities desiredCapabilities = DesiredCapabilities.phantomjs();
        //ssl证书支持
        desiredCapabilities.setCapability("acceptSslCerts", true);
        //截屏支持
        desiredCapabilities.setCapability("takesScreenshot", false);
        //css搜索支持
        desiredCapabilities.setCapability("cssSelectorsEnabled", true);
        desiredCapabilities.setJavascriptEnabled(true);
        //设置参数
        desiredCapabilities.setCapability("phantomjs.page.settings.userAgent", "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:50.0) Gecko/20100101 Firefox/50.0");
        desiredCapabilities.setCapability("phantomjs.page.customHeaders.User-Agent", "Mozilla/5.0 (Windows NT 6.3; Win64; x64; rv:50.0) Gecko/20100101 　　Firefox/50.0");

        //为了提升加载速度，不加载图片
        desiredCapabilities.setCapability("phantomjs.page.settings.loadImages", false);
        //超过10秒放弃加载
        desiredCapabilities.setCapability("phantomjs.page.settings.resourceTimeout", 10000);

        //驱动支持
        if (osname.contains("Linux")) {
            desiredCapabilities.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, linuxExePath);
//            desiredCapabilities.setCapability("webdriver.chrome.driver", driverExePath);
//            desiredCapabilities.setCapability(PhantomJSDriverService.PHANTOMJS_GHOSTDRIVER_PATH_PROPERTY, driverExePath);
        } else {
            desiredCapabilities.setCapability(PhantomJSDriverService.PHANTOMJS_EXECUTABLE_PATH_PROPERTY, winExePath);
//            desiredCapabilities.setCapability("webdriver.chrome.driver", driverExePath);
//            desiredCapabilities.setCapability(PhantomJSDriverService.PHANTOMJS_GHOSTDRIVER_PATH_PROPERTY, driverExePath);
        }
        //是否使用代理
//        if (StringUtils.isNotBlank(proxyProperties.getHost()) && proxyProperties.getPort() != null) {
        Proxy proxy = new Proxy();
        proxy.setProxyType(Proxy.ProxyType.MANUAL);
        proxy.setAutodetect(false);
        String proxyStr = String.format("%s:%d", "ip", 808);
        proxy.setHttpProxy(proxyStr);
        desiredCapabilities.setCapability(CapabilityType.PROXY, proxy);
//        }
        //创建无界面浏览器对象
        return new PhantomJSDriver(desiredCapabilities);
    }

    /**
     * 请求数据
     **/
    public List<Map<String, String>> request() {
        List<Map<String, String>> dataList = new ArrayList<>();
        String url = requestUrl;
        url = String.format(url, System.currentTimeMillis());
        WebDriver webDriver = null;
        try {
            webDriver = getPhantomJs();
            webDriver.manage().timeouts().implicitlyWait(10, TimeUnit.SECONDS);
            webDriver.manage().timeouts().pageLoadTimeout(10, TimeUnit.SECONDS);
            webDriver.get(url);
            TimeUnit.SECONDS.sleep(1);
            WebDriverWait wait = new WebDriverWait(webDriver, 5);
            wait.until(ExpectedConditions.presenceOfElementLocated(By.xpath("/html")));
            Document document = Jsoup.parse(webDriver.getPageSource());
            Elements pointEls = document.select("ul#msg_type li.type_name");
            if (CollectionUtils.isEmpty(pointEls)) {
                return dataList;
            }
            Elements tabEls = document.select("ul#msg_type li.type_jcd");
            if (CollectionUtils.isEmpty(tabEls)) {
                return dataList;
            }
            if (pointEls.size() != tabEls.size()) {
                return dataList;
            }
            if (CollectionUtils.isEmpty(pointMap)) {
                for (int i = 0; i < tabEls.size(); i++) {
                    Element tab = tabEls.get(i);
                    String id = tab.attr("id");
                    Element point = pointEls.get(i);
                    String pointValue = point.text();
                    pointValue = StringUtils.replaceAll(pointValue, " ", "").replaceAll("∨", "");
                    Pattern pattern = Pattern.compile(brackPattern);
                    StringBuffer buffer = new StringBuffer();
                    Matcher matcher = pattern.matcher(pointValue);
                    while (matcher.find()) {
                        matcher.appendReplacement(buffer, "");
                    }
                    matcher.appendTail(buffer);
                    pointValue = buffer.toString();
                    pointMap.put(id, pointValue);
                }
            }
            List<String> titleList = new ArrayList<>(titleMap.keySet());
            dataList = parseData(titleList, tabEls, document);
        } catch (Exception e) {
            logger.error("请求数据出错", e);
            return dataList;
        } finally {
            if (webDriver != null) {
                webDriver.quit();
                webDriver.close();
            }
        }
        return dataList;
    }

    /**
     * 解析数据
     **/
    private List<Map<String, String>> parseData(List<String> titleList, Elements tabEls, Document document) {
        List<Map<String, String>> dataList = new ArrayList<>();
        Date now = Calendar.getInstance().getTime();
        for (Map.Entry<String, String> entry : pointMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            Element tab = Optional.ofNullable(tabEls.select("li#" + key)).orElseGet(() -> document.select("li#" + key)).first();
            Elements tableEls = tab.select("table.num-table");
            if (!CollectionUtils.isEmpty(tableEls)) {
                Element table = tableEls.first();
                Elements trEls = table.select("tr");
                if (!CollectionUtils.isEmpty(trEls)) {
                    for (Element tr : trEls) {
                        Elements tdEls = tr.select("td");
                        Map<String, String> map = new LinkedHashMap<>();
                        for (int i = 0; i < tdEls.size(); i++) {
                            Element td = tdEls.get(i);
                            String tdValue = td.text().replaceAll(" ", "");
                            String title = titleList.get(i);
                            map.put(title, tdValue);
                        }
                        map.put(titleList.get(titleList.size() - 2), value);
                        map.put(titleList.get(titleList.size() - 1), DateFormatUtils.format(now, "yyyy-MM-dd HH:mm:ss"));
                        map.put("type", tableName);
                        dataList.add(map);
                    }
                }
            }
        }
        return dataList;
    }

    public static void main(String[] args) {
        BjmemcRequestDataPhantomJsAndSelenium phantomJs = new BjmemcRequestDataPhantomJsAndSelenium();
        try {
            List<Map<String, String>> mapList = phantomJs.request();
            System.out.println(mapList);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
