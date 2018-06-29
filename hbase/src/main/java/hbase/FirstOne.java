package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class FirstOne {
	
	static Configuration conf = HBaseConfiguration.create();

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort", "2181");

		Connection connection = ConnectionFactory.createConnection(conf);
		Table table = connection.getTable(TableName.valueOf("demo"));
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		
		for(Result res : scanner) {
			System.out.println(res);
			System.out.println(Bytes.toString(res.getRow()));
			System.out.println(Bytes.toString(res.getValue("cf1".getBytes(),"col1".getBytes())));
		}
		scanner.close();
		
		System.out.println("Put Value");
		
		Put put = new Put("3".getBytes());
		
		put.addColumn("cf1".getBytes(), "col1".getBytes(), "value3".getBytes());
		
		table.put(put);
		
		System.out.println("Put Value Completed");
		
		Get get = new Get("4".getBytes());
		
		Result result = table.get(get);
		
		System.out.println("Get Value for Row Key 4:");
		
		System.out.println(Bytes.toString(result.getValue("cf1".getBytes(), "col1".getBytes())));
		
	
		System.out.println("Before Delete row 3");
		
		scanner = table.getScanner(scan);
		 for(Result res:scanner) {
			 System.out.println(Bytes.toString(res.getValue("cf1".getBytes(), "col1".getBytes())));
		 }

		Delete del = new Delete("3".getBytes());
		table.delete(del);
		
		System.out.println("After Delete row 3");
		
		scanner = table.getScanner(scan);
		 for(Result res:scanner) {
			 System.out.println(Bytes.toString(res.getValue("cf1".getBytes(), "col1".getBytes())));
		 }		 
	}

}
