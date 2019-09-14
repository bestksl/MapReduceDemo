package com.ServerAndClients;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-09-13 22:45
 */
public class Server extends Thread {
    int port = 0;
    String name = null;

    public Server(int port, String name) {
        this.port = port;
        this.name = name;
    }

    @Override
    public void run() {

        try {
            ZooKeeper zooKeeper = new ZooKeeper("hadoop-01", 2000, null);
            Stat stat = zooKeeper.exists("/Servers", false);
            if (stat == null) {
                zooKeeper.create("/Servers", "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            zooKeeper.create("/Servers/myserver", name.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            ServerSocket sc = new ServerSocket(port);
            while (true) {
                Socket socket = sc.accept();
                OutputStream outputStream = socket.getOutputStream();
                outputStream.write(("hello from " + this
                        .name + ":" + port).getBytes());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception {

        new Server(2281, "kslServer").start();
        Thread.sleep(10000);
        new Server(2283, "kslServer").start();
        Thread.sleep(5000);
        new Server(2286, "kslServer").start();

    }


}
