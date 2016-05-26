

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;


public class TimeRangeScan {
	
	private static final long TOTAL = 100000;
	
    public static void main (String[] args) throws IOException {
    	
    	//Scan of Read 594000 records took 982 seconds
    	//testTallTableDesign();
    	
    	testTimeSeriesTableDesign();
    	
    }
    
    public static void testTallTableDesign() throws IOException {
    	SORConnection sorConn = new SORConnection("atul0:2181");
    	Connection conn = sorConn.getConnection();    	
    	Table table = conn.getTable(TableName.valueOf("security.events.normalized_1"));
    	
    	long start = System.currentTimeMillis();
    	long counter = 0;
    	
        Scan s = new Scan();
        s.setTimeRange (0L, 1464018664162L);
        ResultScanner scanner = table.getScanner(s);
        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {

            //System.out.println(Bytes.toString(rr.getRow()) + " => " + Bytes.toString(rr.getValue(Bytes.toBytes("b"), Bytes.toBytes("msg"))));
        	
			NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = rr.getMap();
			for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> columnFamilyEntry : map.entrySet()) {
				NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = columnFamilyEntry.getValue();
				
				Map<String, String> event = new HashMap<String, String>();
				for (Entry<byte[], NavigableMap<Long, byte[]>> columnEntry : columnMap.entrySet()) {
					NavigableMap<Long, byte[]> cellMap = columnEntry.getValue();
					for (Entry<Long, byte[]> cellEntry : cellMap.entrySet()) {
						event.put(Bytes.toString(columnEntry.getKey()), Bytes.toString(cellEntry.getValue()));						
					}					
				}
				//System.out.println(event);
				
	        	counter++;	        	
	        	if(counter %1000 == 0){
	        		System.out.println("Read " + counter + " records");
	        	}
	        	
	        	if(counter > TOTAL){
	        		 System.out.println("Took " + (System.currentTimeMillis() - start)/1000 + " seconds to read " + TOTAL + " records from " + table.getName());
	        		 return;
	        	}
	        	
			}
        }
        
        System.out.println("Took " + (System.currentTimeMillis() - start)/1000 + " seconds");
    }
    
    public static void testTimeSeriesTableDesign() throws IOException {
    	SORConnection sorConn = new SORConnection("atul0:2181");
    	Connection conn = sorConn.getConnection();    	
    	Table table = conn.getTable(TableName.valueOf("security.events.normalized_2"));
    	
    	long start = System.currentTimeMillis();
    	long counter = 0;
    	
        Scan s = new Scan(Bytes.toBytes("1_14640"),Bytes.toBytes("1_14641"));
        s.setFilter(new FirstKeyOnlyFilter());
        System.out.println(s.getCaching());
        s.setCaching(500);
        
        ResultScanner scanner = table.getScanner(s);
        for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
        	counter++;
        	
        	if(counter %1000 == 0){
        		System.out.println("Read " + counter + " records");
        	}
        	
            //System.out.println(Bytes.toString(rr.getRow()) + " => " + Bytes.toString(rr.getValue(Bytes.toBytes("b"), Bytes.toBytes("msg"))));
        	//System.out.println(Bytes.toString(rr.getRow()) + " => " + Bytes.toString(rr.value()));
        	
        	
			NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = rr.getMap();
			for (Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> columnFamilyEntry : map.entrySet()) {
				NavigableMap<byte[], NavigableMap<Long, byte[]>> columnMap = columnFamilyEntry.getValue();
								
				for (Entry<byte[], NavigableMap<Long, byte[]>> columnEntry : columnMap.entrySet()) {
					Map<String, String> event = new HashMap<String, String>();
					NavigableMap<Long, byte[]> cellMap = columnEntry.getValue();
					for (Entry<Long, byte[]> cellEntry : cellMap.entrySet()) {
						event.put(Bytes.toString(columnEntry.getKey()), Bytes.toString(cellEntry.getValue()));						
					}	
					//System.out.println(event);
					
		        	counter++;	        	
		        	if(counter %1000 == 0){
		        		System.out.println("Read " + counter + " records");
		        	}
		        	
		        	if(counter > TOTAL){
		        		System.out.println("Took " + (System.currentTimeMillis() - start)/1000 + " seconds to read " + TOTAL + " records from " + table.getName());
		        		 return;
		        	}
				}
	        	
			}
        }
        
        System.out.println("Took " + (System.currentTimeMillis() - start)/1000 + " seconds to read " + counter + " records from " + table.getName());
    }
}