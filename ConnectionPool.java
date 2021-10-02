import java.util.*;
import java.util.concurrent.*;

import org.apache.thrift.*;
import org.apache.thrift.server.*;
import org.apache.thrift.transport.*;
import org.apache.thrift.protocol.*;
public class ConnectionPool {

    // private volatile ArrayBlockingQueue<KeyValueService.Client> connections;
    // ArrayBlockingQueue leads to linearizibility violation
    // A ConcurrentLinkedQueue is an appropriate choice when many threads will share access to a common collection.
    private volatile ConcurrentLinkedQueue<KeyValueService.Client> connections;

    public ConnectionPool(String host, int port, int count) throws Exception {
        this.connections = new ConcurrentLinkedQueue<KeyValueService.Client>();
        for (int i = 0; i < count; i++) {
            this.connections.add(this.createClient(host, port));
        }
    }

    private KeyValueService.Client createClient(String host, int port) throws Exception {
        TSocket socket = new TSocket(host, port);
        TTransport transport = new TFramedTransport(socket);
        transport.open();
        TProtocol protocol = new TBinaryProtocol(transport);
        return new KeyValueService.Client(protocol);
    }

    public KeyValueService.Client getConnection() throws Exception{
        KeyValueService.Client conn = this.connections.poll();
        while (conn == null) {
            conn = this.connections.poll();
        }
        return conn;
    }

    public void offer(KeyValueService.Client client) {
        this.connections.offer(client);
    }
}
