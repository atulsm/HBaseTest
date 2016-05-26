import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author satul

1 2016-05-23-21:17:00 2016-05-23-21:18:00
2 2016-05-23-20:21:00 2016-05-23-22:33:00

snapshot 'security.events.normalized_1', 'security.events.normalized-Snapshot_old'
snapshot 'security.events.normalized_2', 'security.events.normalized-Snapshot_new'

disable 'security.events.normalized_1_delete'
drop 'security.events.normalized_1_delete'
clone_snapshot 'security.events.normalized-Snapshot_old', 'security.events.normalized_1_delete'

disable 'security.events.normalized_2_delete'
drop 'security.events.normalized_2_delete'
clone_snapshot 'security.events.normalized-Snapshot_new', 'security.events.normalized_2_delete'


 */
public class HBaseDelete {

	private static final String DEFAULT_RETENTION_ID = "408E7E50-C02E-4325-B7C5-2B9FE4853476";
	private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH:mm:ss");
	
	public static void main (String[] args) throws Exception {
		System.out.println(new Date(1464022317000L));
		
		if(args.length != 3){
			System.out.println("Usage: java HBaseDelete 1/2 from(eg: 2016-05-23-21:15:25) to(eg:2016-05-23-21:20:35");
			System.exit(0);
		}
		
		if(args[0].equals("1")){
			testTallTableDelete(format.parse(args[1]), format.parse(args[2]));
		}else{
			testTSTableDelete(format.parse(args[1]), format.parse(args[2]));
		}
    }
    
    public static void testTallTableDelete(Date from, Date to) throws IOException {
    	SORConnection sorConn = new SORConnection("atul0:2181");
    	Connection conn = sorConn.getConnection();    	
    	Table table = conn.getTable(TableName.valueOf("security.events.normalized_1_delete"));
    	
    	long start = System.currentTimeMillis();
    	long counter = 0;
    	        
        Scan s = new Scan();
        s.setTimeRange (from.getTime(), to.getTime());
        
        FilterList filterList = new FilterList();

        
        Filter policyFilter = new SingleColumnValueFilter("b".getBytes(), "rv171".getBytes(), CompareFilter.CompareOp.EQUAL, DEFAULT_RETENTION_ID.getBytes());
        filterList.addFilter(policyFilter);

        filterList.addFilter(new FirstKeyOnlyFilter());
        s.setFilter(filterList);        
		
		List<Delete> rowsToDelete = new ArrayList<Delete>();
        ResultScanner scanner = table.getScanner(s);
        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
            //System.out.println(Bytes.toString(rr.getRow()) + " => " + Bytes.toString(rr.value()));
        	rowsToDelete.add(new Delete(rr.getRow()));
        	counter++;	        	
        	if(counter %1000 == 0){
        		System.out.println("Read " + counter + " records");
        	}	        	        	
        }
        System.out.println("Delete Scan took " + (System.currentTimeMillis() - start)/1000 + " seconds to read " + counter + " records");

        
        start = System.currentTimeMillis();
        //table.delete(rowsToDelete);
        System.out.println("Delete took " + (System.currentTimeMillis() - start)/1000 + " seconds to delete " + counter + " records");
    }
    
    public static void testTSTableDelete(Date from, Date to) throws Exception {
    	SORConnection sorConn = new SORConnection("atul0:2181");
    	Connection conn = sorConn.getConnection();    	
    	Table table = conn.getTable(TableName.valueOf("security.events.normalized_2_delete"));
    	
    	long start = System.currentTimeMillis();
    	long counter = 0;
    	String policyId = getPolicyKey(DEFAULT_RETENTION_ID);  
    	
    	System.out.println(policyId);
    	long begin = from.getTime()/1000;
    	long end = to.getTime()/1000;
    	
    	List<Delete> rowsToDelete = new ArrayList<Delete>();
    	for (;begin<=end;begin++) {
    		for(int i=0;i<=16;i++){
    			String rowId = Integer.toHexString(i).toUpperCase() + "_"  + begin + "_" + policyId;
    			rowsToDelete.add(new Delete(rowId.getBytes()));
    			counter++;
    		}        	
        }
    	
    	//table.delete(rowsToDelete);
        System.out.println("Took " + (System.currentTimeMillis() - start)/1000 + " seconds to delete " + counter + " records");
    }
    
	protected static String getPolicyKey(String policyIdStr) throws Exception {
		byte[] digest = MessageDigest.getInstance("MD5").digest(Bytes.toBytes(policyIdStr));
		ByteBuffer wrapped = ByteBuffer.wrap(digest);		
		int num = Math.abs(wrapped.getShort()%10000);
		String formatted = String.format("%04d", num);
		return formatted;
	}

    
    
}
