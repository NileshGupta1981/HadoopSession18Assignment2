package HbaseDML;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class DataInsertion {
	

				Configuration conf=HBaseConfiguration.create();
		public void insert(String tableName, String rowKey,
	            String family, String qualifier, String value) throws Exception {
	        try {
	            HTable table = new HTable(conf, tableName);
	            Put put = new Put(Bytes.toBytes(rowKey));
	            put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
	                    .toBytes(value));
	            table.put(put);

	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	    }
		
	
		public static void main(String[] args) throws Exception {
			DataInsertion dml=new DataInsertion();
			String tablename="company";
			
			

			// first record
		    dml.insert(tablename, "e1", "employee", "id", "102");
		    dml.insert(tablename, "e1", "employee", "name", "raj");
		    dml.insert(tablename, "e1", "employee", "age", "29");
		    //second record
		    dml.insert(tablename, "e2", "employee", "id", "103");
		    dml.insert(tablename, "e2", "employee", "name", "mohan");
		    dml.insert(tablename, "e2", "employee", "age", "25");
		    //third record
		    dml.insert(tablename, "e3", "employee", "id", "104");
		    dml.insert(tablename, "e3", "employee", "name", "priya");
		    dml.insert(tablename, "e3", "employee", "age", "27");

			
		    
		}
	}	


