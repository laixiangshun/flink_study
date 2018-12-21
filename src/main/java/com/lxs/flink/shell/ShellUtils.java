package com.lxs.flink.shell;

import com.jcraft.jsch.*;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * java 远程操作linux
 * 读取文件数据
 **/
public class ShellUtils {

    private static Session session;

    public static void main(String[] args) {
        String host = "192.168.20.48";
        String userName = "root";
        String password = "root";
        Integer port = 22;
        String command = "cat /data/test";
        try {
            System.out.println(execCmd(command, host, userName, password, port));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void connect(String host, Integer port, String userName, String password) throws Exception {
        JSch jSch = new JSch();
        session = jSch.getSession(userName, host, port);
        if (session == null) {
            throw new Exception("连接主机为" + host + "的linux失败");
        }
        session.setPassword(password);
        Properties prop = new Properties();
        //第一次登陆
        prop.put("StrictHostKeyChecking", "no");
        session.setConfig(prop);
        session.connect(3000);
    }

    private static String execCmd(String command, String host, String userName, String password, Integer port) throws Exception {
        System.out.println(command);
        connect(host, port, userName, password);
        BufferedReader reader = null;
        Channel channel = null;
        StringBuilder buffer = new StringBuilder();
        try {
            channel = session.openChannel("exec");
            ChannelExec exec = (ChannelExec) channel;
            exec.setCommand(command);
            channel.setInputStream(null);
            exec.setErrStream(System.err);
            exec.connect();
            InputStream in = exec.getInputStream();
            reader = new BufferedReader(new InputStreamReader(in));
            String buf;
            while ((buf = reader.readLine()) != null) {
                buffer.append(buf).append("\n");
            }
        } catch (JSchException e) {
            e.printStackTrace();
            throw e;
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (channel != null) {
                channel.disconnect();
            }
            session.disconnect();
        }
        return buffer.toString();
    }
}
