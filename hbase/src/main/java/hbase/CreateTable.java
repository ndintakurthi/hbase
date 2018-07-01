package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Scan;

public class CreateTable {

	//Create configuration
	static Configuration conf = HBaseConfiguration.create();
	
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		//Set configuration
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
		//Create connection
		Connection connection = ConnectionFactory.createConnection(conf);
		
		//get HBase admin
		//HBaseAdmin admin1 = new HBaseAdmin(connection);
		
		Admin admin = connection.getAdmin();
		
		// Get Table Descriptor
		HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf("demo2"));
		
		HColumnDescriptor columnFamily = new HColumnDescriptor("cf1".getBytes());
		
		tableDesc.addFamily(columnFamily);
		
		admin.createTable(tableDesc);
	
		
		// Get Table
		Table table =connection.getTable(TableName.valueOf("demo2"));
		
		Put firstRow = new Put("1".getBytes());
		
		firstRow.addColumn("cf1".getBytes(), "col1".getBytes(), "value1".getBytes());	
		
		table.put(firstRow);
		
		// Define Scanner object
		
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		
		for(Result res:scanner) {
			//System.out.println(res);
			
			System.out.println(Bytes.toString(res.getValue("cf1".getBytes(), "col1".getBytes())));
		}
		
		Put put = new Put("3".getBytes());
		
		put.addColumn("cf1".getBytes(), "col1".getBytes(), "value3".getBytes());
		
		table.put(put);
		
		Get get = new Get("3".getBytes());
		
		Result result = table.get(get);
		
		System.out.println(Bytes.toString(result.getValue("cf1".getBytes(), "col1".getBytes())));
		
		Delete del = new Delete("3".getBytes());
		
		table.delete(del);
		
		scanner=table.getScanner(scan);
		for(Result res:scanner) {
			System.out.println(Bytes.toString(res.getValue("cf1".getBytes(), "col1".getBytes())));
		}
		
		
		//admin.disableTable(TableName.valueOf("demo2"));
		//admin.deleteTable(TableName.valueOf("demo2"));
		
	}

}
