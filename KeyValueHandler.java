import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.*;
import org.apache.curator.*;
import org.apache.curator.retry.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.api.*;

import com.google.common.util.concurrent.Striped;

import org.apache.log4j.*;

public class KeyValueHandler implements KeyValueService.Iface, CuratorWatcher{
    private Map<String, String> myMap;
    private CuratorFramework curClient;
    private String zkNode;
    private String host;
    private int port;

    // from server - maximum thread count is 64, 32 seems having a good performance
    private final int CLIENT_CONNECTIONS = 32;
    private volatile ConcurrentLinkedQueue<KeyValueService.Client> connections = null;
    
    private static Logger logger;
    private volatile Boolean isPrimary = false;

    // private ConnectionPool connectionPool = null;
    
    // ReentrantLock is basically a mutex - "A ReentrantLock is owned by the thread last successfully locking, but not yet unlocking it."
    private ReentrantLock mutex = new ReentrantLock();
    // from google guava - this is object-level locking
    // it guarantees that if key1 == key2, locks.get(key1) = locks.get(key2)
    // cannot be too low - otherwise the above is not guaranteed
    private Striped<Lock> objLocks = Striped.lock(2*CLIENT_CONNECTIONS);

    public KeyValueHandler(String host, int port, CuratorFramework curClient, String zkNode) throws Exception {
        this.host = host;
        this.port = port;
        this.curClient = curClient;
        this.zkNode = zkNode;

        this.logger = Logger.getLogger(KeyValueHandler.class.getName());
        curClient.sync();
        List<String> workers = curClient.getChildren().usingWatcher(this).forPath(zkNode);
        if (workers.size() != 1) {
            // find out if this is primary or not in a rigorous way
            // unless ip:port is the same, it is not primary
            Collections.sort(workers);
            String replicaString = new String(this.curClient.getData().forPath(zkNode + "/" + workers.get(workers.size() - 1)));
            int idx = replicaString.indexOf(':');
            String rHost = replicaString.substring(0, idx);
            int rPort = Integer.parseInt(replicaString.substring(idx+1));
            this.isPrimary = !(this.host.equals(rHost) && this.port == rPort) ? true : false;
        } else {
            this.isPrimary = true;
        }

        myMap = new ConcurrentHashMap<String, String>();
    }

    private KeyValueService.Client createClient(String host, int port) throws org.apache.thrift.TException {
        TSocket socket = new TSocket(host, port);
        TTransport transport = new TFramedTransport(socket);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        return new KeyValueService.Client(protocol);
    }

    synchronized public void process(WatchedEvent event) throws org.apache.thrift.TException {
        try {
            this.curClient.sync();
            List<String> workers = this.curClient.getChildren().usingWatcher(this).forPath(zkNode);
            // if only one primary is present, don't create replica
            if (workers.size() == 1) {
                this.isPrimary = true;
                return;
            }
            // find out if this is primary or not in a rigorous way
            // unless ip:port is the same, it is not primary
            Collections.sort(workers);
            String replicaString = new String(this.curClient.getData().forPath(zkNode + "/" + workers.get(workers.size() - 1)));
            int idx = replicaString.indexOf(':');
            String rHost = replicaString.substring(0, idx);
            int rPort = Integer.parseInt(replicaString.substring(idx+1));
            this.isPrimary = !(this.host.equals(rHost) && this.port == rPort) ? true : false;
            
            if (this.isPrimary && this.connections == null) {
                KeyValueService.Client client = null;

                while(client == null) {
                    try {
                        TSocket sock = new TSocket(rHost, rPort);
                        TTransport transport = new TFramedTransport(sock);
                        transport.open();
                        TProtocol protocol = new TBinaryProtocol(transport);
                        client = new KeyValueService.Client(protocol);
                    } catch (Exception e) {}
                }
                
                // create replica by locking the whole hashmap
                mutex.lock();
                client.migrateToReplica(this.myMap);
                // create connection pool for worker threads
                this.connections = new ConcurrentLinkedQueue<KeyValueService.Client>();
                for (int i = 0; i < this.CLIENT_CONNECTIONS; i++) {
                    this.connections.add(this.createClient(rHost, rPort));
                }
                // KeyValueService.Client client = this.connectionPool.getConnection();
                // client.migrateToReplica(this.myMap);
                // this.connectionPool.offer(client);
                this.mutex.unlock();
            } else {
                this.connections = null;
            }
        } catch (Exception e) {
            this.connections = null;
        }
    }

    public void assignPrimary() throws org.apache.thrift.TException {
        this.isPrimary = true;
    }

    public void ping() throws org.apache.thrift.TException { this.assignPrimary(); }

    public String get(String key) throws org.apache.thrift.TException {
        // After extensive testing, there's still latency when reading from the replica.
        // The replica might not have been updated after a put;
        // Hence, throw exception and only read from the primary to ensure correctness.
        if (this.isPrimary == false) {
            throw new org.apache.thrift.TException("Please wait for primary to come back up.");
        }

        try {
            String ret = myMap.get(key);
            if (ret == null)
                return "";
            else
                return ret;
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    // Idea is to put into primary first and copy to replica
    public void put(String key, String value) throws org.apache.thrift.TException {
        if (this.isPrimary == false) {
            throw new org.apache.thrift.TException("Please wait for primary to come back up.");
        }

        Lock lock = this.objLocks.get(key);
        lock.lock();
        while (mutex.isLocked()){}
        try {
            // we save to primary first, and copy to replica
            myMap.put(key, value);
            if (this.connections != null) {
          
                KeyValueService.Client conn = this.connections.poll();
                while (conn == null) { conn = this.connections.poll(); }
                
                conn.putToReplica(key, value);
                this.connections.offer(conn);
            }
        } catch (Exception e) {
            e.printStackTrace();
            this.connections = null;
        } finally {
            lock.unlock();
        }
    }

    // propagating the put from primary to replica - called as an RPC from primary
    public void putToReplica(String key, String value) throws org.apache.thrift.TException {
        Lock lock = objLocks.get(key);
        lock.lock();

        try {
            myMap.put(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }
    
    // an RPC from primary for replication
    public void migrateToReplica(Map<String, String> map) throws org.apache.thrift.TException {
        this.myMap = new ConcurrentHashMap<String, String>(map); 
    }
}
