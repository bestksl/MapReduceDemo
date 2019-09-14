package com.ServerAndClients;


import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-09-13 23:08
 */
public class Client {
    private ZooKeeper zooKeeper = null;

    private void getServerInfo() throws Exception {
        zooKeeper = new ZooKeeper("hadoop-01", 2000, watchedEvent -> {
            if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected && watchedEvent.getType() == Watcher.Event.EventType.NodeChildrenChanged) {
                try {
                    System.out.println("服务器退出");
                    getServers();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

    }


    private void getServers() throws Exception {
        List serverList = new ArrayList();
        System.out.println(zooKeeper.getChildren("/Servers", true));
        System.out.println("服务器列表: " + serverList);
    }

    private void getData() throws IOException {
        Socket socket = new Socket();
        SocketAddress socketAddress = new InetSocketAddress(2281);
        socket.connect(socketAddress, 2000);
        InputStream inputStream = socket.getInputStream();
        byte[] buffer = new byte[50];
        while (inputStream.read(buffer) != -1) {
            System.out.println(new String(buffer, StandardCharsets.UTF_8));
        }
    }


    public static void main(String[] args) throws Exception {
        Client client1 = new Client();
        client1.getData();
        client1.getServerInfo();
        client1.getServers();
        Thread.sleep(Long.MAX_VALUE);
    }

}
