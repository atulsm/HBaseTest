

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class SORConnection{

	private Connection connection;
	private boolean initialized = false;

	public SORConnection(final String zookeeper) {
		Configuration config = HBaseConfiguration.create();
		config.set(HConstants.ZOOKEEPER_QUORUM, zookeeper);
		config.set(HConstants.HBASE_CLIENT_RETRIES_NUMBER, String.valueOf(24*60*60*1000));
				
		try{
			connection = ConnectionFactory.createConnection(config);
			initialized = true;
		}catch(Throwable th) {
			th.printStackTrace();
		}
	}

	public Connection getConnection() {
		return connection;
	}
	
	public boolean isInitialized(){
		return initialized;
	}
}