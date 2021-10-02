import java.io.*;
import java.util.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.utils.*;

import org.apache.log4j.*;

public class MonitorThread extends Thread{
    String zkNode;
    CuratorFramework curClient;
    static Logger log;

    final int PING_INTERVAL = 30;

    public MonitorThread(String zkNode, CuratorFramework curClient) {
        this.zkNode = zkNode;
        this.curClient = curClient;
        this.log = Logger.getLogger(StorageNode.class.getName());
    }

    private KeyValueService.Client createClient(String host, int port) throws Exception {
        TSocket socket = new TSocket(host, port);
        TTransport transport = new TFramedTransport(socket);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        return new KeyValueService.Client(protocol);
    }
    
    @Override
    public void run() {
        try {
            // first step is to get all worker znodes
            this.curClient.sync();
            List<String>workers = this.curClient.getChildren().forPath(this.zkNode);
            while (workers.size() == 0) {
                this.curClient.sync();
                workers = this.curClient.getChildren().forPath(this.zkNode);
            } 
            
            if (workers.size() == 1) {return;}

            // we have one primary and one backup
            if (workers.size() != 1) {
                Collections.sort(workers);
                // if we have a backup, we get backup data
                String secondary = workers.get(workers.size()-1);
                String sdata = new String(this.curClient.getData().forPath(this.zkNode+"/"+secondary));
                int sindex = sdata.indexOf(':');
                String shost = sdata.substring(0, sindex);
                int sport = Integer.parseInt(sdata.substring(sindex+1));

                // needs to find out if we always have just two znodes - one primary and one secondary
                // store primary info for periodically pinging
                String primary = workers.get(workers.size()-2);
                String pdata = new String(this.curClient.getData().forPath(this.zkNode+"/"+primary));
                int pindex = pdata.indexOf(':');
                String phost = pdata.substring(0, pindex);
                int pport = Integer.parseInt(pdata.substring(pindex+1));
                KeyValueService.Client pclient = this.createClient(phost, pport);

                // periodically check if primary is up
                log.info("Checking if primary is up...");
                while (true) {
                    try {
                        Thread.sleep(PING_INTERVAL);
                        pclient.ping();
                    } catch (Exception e) {
                        break;
                    }
                }

                // primary failed - delete and make replica primary
                log.info("Primary failover...");
                this.curClient.delete().forPath(this.zkNode+"/"+primary);
                KeyValueService.Client sclient = this.createClient(shost, sport);
                sclient.assignPrimary();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
