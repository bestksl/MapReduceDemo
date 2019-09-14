package com.ZooKeeperClientDemo;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLOutput;
import java.util.List;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-09-13 00:52
 */
public class ZooKeeperCommands {
    private ZooKeeper zooKeeper;


//    public void testCreate() throws IOException, KeeperException, InterruptedException {
//        zooKeeper = new ZooKeeper("hadoop-01", 2000, null);
//        String a = zooKeeper.create("/ksll", "carry on!".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        System.out.println(a);
//        System.out.println(zooKeeper.setData("/ksll", "666".getBytes(), -1));
//        byte[] bytes = zooKeeper.getData("/ksll", false, null);
//        String result = new String(bytes, "utf-8");
//        System.out.println(result);
//    }

    private void testWatcher() throws Exception {
        zooKeeper = new ZooKeeper("hadoop-01", 2000, new Watcher1());
        byte[] a = zooKeeper.getData("/zookeeper", true, null);
        List<String> b = zooKeeper.getChildren("/zookeeper", true);
        Thread.sleep(Long.MAX_VALUE);


    }

    class Watcher1 implements Watcher {

        @Override
        public void process(WatchedEvent watchedEvent) {

            if (watchedEvent.getState().equals(Event.KeeperState.SyncConnected) && watchedEvent.getType() == Event.EventType.NodeDataChanged) {
                System.out.println(watchedEvent.getPath());
                System.out.println(watchedEvent.getType());
                System.out.println(watchedEvent.toString());
                try {
                    System.out.println(new String(zooKeeper.getData("/zookeeper", true, null), StandardCharsets.UTF_8));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else if (watchedEvent.getState().equals(Event.KeeperState.SyncConnected) && watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
                System.out.println("子节点改变");
                System.out.println(watchedEvent.getPath());
                System.out.println(watchedEvent.getType());
                System.out.println(watchedEvent.toString());
                try {
                    System.out.println(zooKeeper.getChildren("/zookeeper", true));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        new ZooKeeperCommands().testWatcher();
    }
}
