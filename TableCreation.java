package HbaseDDL;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;


public class TableCreation {
	
		
	Configuration conf=HBaseConfiguration.create();

		public void createtable(String name,String[] colfamily) throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
			
			
			HBaseAdmin admin=new HBaseAdmin(conf);
			HTableDescriptor des=new HTableDescriptor(Bytes.toBytes(name));
			
			for (int i = 0; i < colfamily.length; i++) {
				des.addFamily(new HColumnDescriptor(colfamily[i]));
		    }
			
			if(admin.tableExists(name)){
				System.out.println("Table Already exists");
			}
			else{
				admin.createTable(des);	
				System.out.println("Table:"+ name +" sucessfully created");
			}
		}
		
			
		public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
			TableCreation ddl=new TableCreation();
			String tablename = "company";
		    String[] familys = { "employee" };
		    ddl.createtable(tablename, familys);
		    //ddl.deleteTable(tablename);
		}
	}


