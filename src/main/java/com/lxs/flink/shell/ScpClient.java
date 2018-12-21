package com.lxs.flink.shell;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.Session;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Scp 命令获取linux文件和上传文件到linux
 **/
public class ScpClient {
    public static void main(String[] args) {
        String host = "192.168.20.48";
        String userName = "root";
        String password = "root";
        Integer port = 22;
        ScpClient scp = ScpClient.getInstance(host, port, userName, password);
        File file = new File("C:\\Users\\hasee\\Desktop\\spark.txt");
        if (!file.exists()) {
            System.err.println("文件不存在");
            System.exit(1);
        }
        String name = file.getName();
        name = StringUtils.substring(name, 0, name.lastIndexOf("."));
        scp.putFile(file.getAbsolutePath(), name + ".tmp", "/data", null);
        scp.getFile("/data/spark", "C:\\Users\\hasee\\Desktop");
    }

    private String ip;
    private int port;
    private String username;
    private String password;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    static private ScpClient instance;

    public static synchronized ScpClient getInstance(String IP, int port, String username, String passward) {
        if (instance == null) {
            instance = new ScpClient(IP, port, username, passward);
        }
        return instance;
    }

    public ScpClient(String IP, int port, String username, String passward) {
        this.ip = IP;
        this.port = port;
        this.username = username;
        this.password = passward;
    }

    public void getFile(String remoteFile, String localTargetDirectory) {
        Connection conn = new Connection(ip, port);
        try {
            conn.connect();
            boolean isAuthenticated = conn.authenticateWithPassword(username, password);
            if (!isAuthenticated) {
                System.err.println("authentication failed");
            }
            SCPClient client = new SCPClient(conn);
            client.get(remoteFile, localTargetDirectory);
            conn.close();
        } catch (IOException ex) {
            Logger.getLogger(SCPClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }


    public void putFile(String localFile, String remoteTargetDirectory) {
        Connection conn = new Connection(ip, port);
        try {
            conn.connect();
            boolean isAuthenticated = conn.authenticateWithPassword(username, password);
            if (!isAuthenticated) {
                System.err.println("authentication failed");
            }
            SCPClient client = new SCPClient(conn);
            client.put(localFile, remoteTargetDirectory);
            conn.close();
        } catch (IOException ex) {
            Logger.getLogger(SCPClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }


    public void putFile(String localFile, String remoteFileName, String remoteTargetDirectory, String mode) {
        Connection conn = new Connection(ip, port);
        try {
            conn.connect();
            boolean isAuthenticated = conn.authenticateWithPassword(username, password);
            if (!isAuthenticated) {
                System.err.println("authentication failed");
            }
            SCPClient client = new SCPClient(conn);
            if ((mode == null) || (mode.length() == 0)) {
                mode = "0600";
            }
            client.put(localFile, remoteFileName, remoteTargetDirectory, mode);

            //重命名
            Session sess = conn.openSession();
            String tmpPathName = remoteTargetDirectory + File.separator + remoteFileName;
            String newPathName = tmpPathName.substring(0, tmpPathName.lastIndexOf("."));
            //重命名回来
            sess.execCommand("mv " + remoteFileName + " " + newPathName);
            conn.close();
        } catch (IOException ex) {
            Logger.getLogger(SCPClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public void putFile(String localFile, String remoteFileName, String remoteTargetDirectory) {
        Connection conn = new Connection(ip, port);
        try {
            conn.connect();
            boolean isAuthenticated = conn.authenticateWithPassword(username,
                    password);
            if (!isAuthenticated) {
                System.err.println("authentication failed");
            }
            SCPClient client = new SCPClient(conn);
            client.put(getBytes(localFile), remoteFileName, remoteTargetDirectory);
            conn.close();
        } catch (IOException ex) {
            Logger.getLogger(SCPClient.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static byte[] getBytes(String filePath) throws IOException {
        byte[] buffer = null;
        File file;
        FileInputStream fis = null;
        ByteArrayOutputStream byteArray = null;
        try {
            file = new File(filePath);
            fis = new FileInputStream(file);
            byteArray = new ByteArrayOutputStream(1024 * 1024);
            byte[] b = new byte[1024 * 1024];
            int i;
            while ((i = fis.read(b)) != -1) {
                byteArray.write(b, 0, i);
            }
            byteArray.flush();
            buffer = byteArray.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fis != null) {
                fis.close();
            }
            if (byteArray != null) {
                byteArray.close();
            }
        }
        return buffer;
    }
}
