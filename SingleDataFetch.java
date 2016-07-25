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

public class SingleDataFetch {

		Configuration conf=HBaseConfiguration.create();
				
		public void getrecord(String tablename,String rowkey) throws IOException{
			HTable table=new HTable(conf, tablename);
			Get get=new Get(Bytes.toBytes(rowkey));
			Result rs=table.get(get);
			
			for(KeyValue kv : rs.raw()){
		        System.out.print(new String(kv.getRow()) + " " );
		        System.out.print(new String(kv.getFamily()) + ":" );
		        System.out.print(new String(kv.getQualifier()) + " " );
		        System.out.print(kv.getTimestamp() + " " );
		        System.out.println(new String(kv.getValue()));
		    }
		}
		

		
		public static void main(String[] args) throws Exception {
			SingleDataFetch dml=new SingleDataFetch();
			String tablename="company";
			
			
			
		    //display one record
		    dml.getrecord("company", "e2");
		 
		}
	}
