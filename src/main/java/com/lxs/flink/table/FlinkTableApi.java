package com.lxs.flink.table;

import com.lxs.flink.model.Student;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;

import java.io.File;
import java.io.IOException;

/**
 * Flink SQL 的 API 操作
 * 可将本地文件换成hdfs
 **/
public class FlinkTableApi {

    private static final String inPath = "C:\\Users\\hasee\\Desktop\\student.csv";
    private static final String outPath = "C:\\Users\\hasee\\Desktop\\result.csv";

    public static void main(String[] args) {
        //获取Flink系统对象
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        //读取数据源并注册为表
        String[] filedNames = new String[]{"id", "name", "password", "age"};
        TypeInformation<?>[] filedTypes = new TypeInformation[]{Types.INT(), Types.STRING(), Types.STRING(), Types.INT()};
        CsvTableSource csvTableSource = new CsvTableSource(inPath, filedNames, filedTypes);
        tableEnv.registerTableSource("student", csvTableSource);
        Table student = tableEnv.scan("student");

        //将表转换为数据集
        DataSet<Student> dataSet = tableEnv.toDataSet(student, Student.class);
        tableEnv.registerDataSet("student2", dataSet, "id,name,password,age");
        Table result = tableEnv.sqlQuery("select age,count(id) as num from student2 group by age order by age");

        File out = new File(outPath);
        if (!out.exists()) {
            try {
                out.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //保存结果
        TableSink<?> tableSink = new CsvTableSink(outPath, "|", 1, FileSystem.WriteMode.OVERWRITE);
        result.writeToSink(tableSink);
        //设置并发数
        env.setParallelism(1);
        try {
            env.execute("flink table count");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
