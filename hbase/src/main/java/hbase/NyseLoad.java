package hbase;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;

public class NyseLoad {

	static Configuration conf = HBaseConfiguration.create();
	
	public static Put buildPutList(NyseParser parser) {
		
		SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy");
		String transactionDate=null;		
		try {
			transactionDate = new SimpleDateFormat("yyyy-MM-dd")
					          .format(formatter.parse(parser.getTransactionDate()))
					          .toString();
		}catch(ParseException e) {
			e.printStackTrace();
		}
		
		Put put = new Put((parser.getStockTicker()+","+transactionDate).getBytes());
		put.addColumn("sd".getBytes(), "op".getBytes(), Bytes.toBytes(parser.getOpenPrice().floatValue()));
		put.addColumn("sd".getBytes(), "hp".getBytes(), Bytes.toBytes(parser.getHighPrice().floatValue()));
		put.addColumn("sd".getBytes(), "lp".getBytes(), Bytes.toBytes(parser.getLowPrice().floatValue()));
		put.addColumn("sd".getBytes(), "cp".getBytes(), Bytes.toBytes(parser.getClosePrice().floatValue()));
		put.addColumn("sd".getBytes(), "v".getBytes(), Bytes.toBytes(parser.getVolume().intValue()));
		
		return put;
	}
	
	public static void LoadData(Table table,String path) {
		
		
		
		File inputFolder = new File(path);
		File[] files = inputFolder.listFiles();
		for(File file:files) {
			System.out.println("File Name:"+file.getName());
			BufferedReader br = null;
			try {
				String currentLine;
				br = new BufferedReader(new FileReader(file));
				List<Put> puts = new ArrayList<Put>();
				while((currentLine=br.readLine()) != null) {
					NyseParser nyseParser = new NyseParser();
					nyseParser.parse(currentLine);
					puts.add(buildPutList(nyseParser));
				}
				table.put(puts);
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		conf.set("hbase.zookeeper.quorum", "localhost");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		
		Connection connection = ConnectionFactory.createConnection(conf);
		
		Table table = connection.getTable(TableName.valueOf("nyse"));
		
		System.out.println("Start Loading Data");
		
		LoadData(table,args[0]);
		
		System.out.println("End Loading Data");
	}

}
