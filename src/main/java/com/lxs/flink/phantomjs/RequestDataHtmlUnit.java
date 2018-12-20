package com.lxs.flink.phantomjs;

import com.gargoylesoftware.htmlunit.WebClient;
import com.lxs.flink.utils.DBManager;
import com.lxs.flink.utils.JdbcUtil;
import com.lxs.flink.utils.WebClintUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.chrome.ChromeDriver;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.CollectionUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 爬取北京空气质量 监测点空气质量指数
 * htmlunit
 **/

public class RequestDataHtmlUnit {
    private static final String requestUrl = "http://zx.bjmemc.com.cn/getAqiList.shtml?timestamp=%d";
    private static final String referer = "http://zx.bjmemc.com.cn/getAqiList.shtml";
    private static Map<String, String> titleMap = new LinkedHashMap<>();
    private static Map<String, String> pointMap = new LinkedHashMap<>();
    /**
     * 匹配括号及其内容
     */
    private static String brackPattern = "(\\(|（)[^(\\(|（)]+(\\)|）)";
    private static final String tableName = "air_quality_point";
    private static final String tableComment = "北京空气质量监测点空气质量指数";

    static {
        titleMap.put("site", "站点");
        titleMap.put("air_quality", "空气质量");
        titleMap.put("aqi", "AQI");
        titleMap.put("primary_pollutant", "首要污染物");
        titleMap.put("evaluation_point", "评价点");
        titleMap.put("data_time", "数据时间");
    }


    public List<Map<String, String>> request() throws Exception {
        List<Map<String, String>> dataList = new ArrayList<>();
        String url = requestUrl;
        url = String.format(url, System.currentTimeMillis());
        Map<String, String> headerMap = new HashMap<>();
        headerMap.put("Cookie", "JSESSIONID=55BE9AD8CF38CDA4D63DCE1F0F928F4D");
        WebClient webClient = WebClintUtils.getWebClient(null, headerMap, url, referer);
//        if (StringUtils.isNotBlank(proxyProperties.getHost()) && proxyProperties.getPort() != null) {
//            WebClintUtils.configProxy(webClient, proxyProperties.getHost(), proxyProperties.getPort(), proxyProperties.getUserName(), proxyProperties.getPassword());
//        }
        String content = WebClintUtils.getWebContent(webClient, url, 1, 3);
        if (StringUtils.isBlank(content)) {
            return dataList;
        }
        Document document = Jsoup.parse(content);
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
                pointValue = StringUtils.replaceAll(pointValue, " ", "");
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

//        boolean tableExists = tableIfExists();
//        if (!tableExists) {
//            String createSql = buildCreateSql(titleMap);
//            JdbcUtil.getJdbcTemplate().execute(createSql);
//        }

        Date now = Calendar.getInstance().getTime();
        List<String> titleList = new ArrayList<>(titleMap.keySet());
        for (Map.Entry<String, String> entry : pointMap.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            Element tab = Optional.ofNullable(tabEls.attr("id", key)).orElseGet(() -> document.select("li#" + key)).first();
//            Elements tabEles = document.select("li#"+key+" table.num-table");
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
        webClient.close();
        return dataList;
    }

    private String buildCreateSql(Map<String, String> headMap) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS").append('`').append(tableName).append("`(");
        sql.append("`id` ").append("int(11) ").append("NOT NULL ").append("AUTO_INCREMENT ").append("COMMENT '").append("主键").append("',");
        for (Map.Entry<String, String> entry : headMap.entrySet()) {
            String colName = entry.getValue();
            String comment = entry.getKey();
            sql.append("`").append(colName).append("` ").append("varchar(255) ").append("DEFAULT NULL ").append("COMMENT '").append(comment).append("',");
        }
        sql.append("PRIMARY KEY (`id`) USING BTREE");
        sql.append(")").append("ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='").append(tableComment).append("'");
        return sql.toString();
    }

    /**
     * 判断表是否已经存在，如果已经存在则删除
     */
    private boolean tableIfExists() {
        boolean exists = false;
        try {
//            String sql = String.format(" show tables like '%s'", table_name);
            Connection connection = DBManager.connection();
            DatabaseMetaData metaData = connection.getMetaData();
            String[] type = {"TABLE"};
            ResultSet result = metaData.getTables(null, null, tableName, type);
            exists = result.next();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return exists;
    }

    private String buildInsertSql(List<String> headList) {
        StringBuilder builder = new StringBuilder("INSERT INTO ");
        StringBuilder questionMarks = new StringBuilder(") VALUES (");
        builder.append("`").append(tableName).append("`").append("(");
        int i = 0;
        for (String column : headList) {
            if (i++ > 0) {
                builder.append(", ");
                questionMarks.append(", ");
            }
            builder.append("`").append(column).append("`");
            questionMarks.append(':').append(column);
        }
        builder.append(questionMarks).append(")");
        return builder.toString();
    }

    /**
     * 批量插入
     */
    private static void insertExcelDataBatch(String insertSql, List<Map<String, String>> contentMap) throws RuntimeException {
        JdbcTemplate jdbcTemplate = JdbcUtil.getJdbcTemplate();
        NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jdbcTemplate.getDataSource());
        Map<String, String>[] maps = contentMap.toArray(new Map[contentMap.size()]);
        int[] batchUpdate = namedParameterJdbcTemplate.batchUpdate(insertSql, maps);
        for (int i : batchUpdate) {
            if (i != 1) {
                throw new RuntimeException("写入数据出错！");
            }
        }

    }

    public static void main(String[] args) {
//        try {
//            System.out.println(new RequestData().request());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
        System.getProperties().setProperty("webdriver.chrome.driver",
                "D:\\360极速浏览器下载\\chromedriver_win32\\chromedriver.exe");
        WebDriver webDriver = new ChromeDriver();
        String url = requestUrl;
        url = String.format(url, System.currentTimeMillis());
        webDriver.get(url);
        WebElement webElement = webDriver.findElement(By.xpath("/html"));
        System.out.println(webElement.getAttribute("outerHTML"));
        webDriver.close();
    }
}
