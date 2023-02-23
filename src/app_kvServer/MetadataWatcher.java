package app_kvServer;
import org.apache.zookeeper.*;

// A watcher that watches the path /metadata and print out the updated value
public class MetadataWatcher implements Watcher{
    private ZooKeeper zk;
    private String metadataPath;
    private String metadataValue;

    public MetadataWatcher(String metadataPath) {
        try {
            this.zk = new ZooKeeper("localhost:2181", 5000, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        this.metadataPath = metadataPath;
        try {
            this.metadataValue = new String(zk.getData(metadataPath, this, null));
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeDataChanged) {
            try {
                this.metadataValue = new String(zk.getData(metadataPath, this, null));
                System.out.println("Metadata value changed to: " + this.metadataValue);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public String getMetadataValue() {
        return this.metadataValue;
    }
}
