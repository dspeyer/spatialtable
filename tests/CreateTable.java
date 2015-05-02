import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class CreateTable {

    public static String filename;
    public static String tablename;
     
    public static void main(String[] args) throws IOException{
	if (args.length>0) {
	    filename=args[0];
	} else {
	    filename="starbucks.csv";
	}
	String[] pieces=filename.split("\\.");
	System.out.println("filename="+filename);
	System.out.println("pieces.length="+pieces.length);
	tablename=pieces[0];
	//Instantiating configuration class
	Configuration con = HBaseConfiguration.create();
	//Instantiating HBaseAdmin class
	HBaseAdmin admin = new HBaseAdmin(con);
	//Instantiating table descriptor class
	HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tablename));
	//Adding column families to table descriptor
	tableDescriptor.addFamily(new HColumnDescriptor("StoreInformation"));
	admin.createTable(tableDescriptor);
	CreateTable obj = new CreateTable();
	obj.run();
	//Execute the table through admin
	System.out.println("Table created");
    }
    
    public void run() throws IOException{
	//Instantiating configuration class
	// Configuration con = HBaseConfiguration.create();
        //Instantiating HTable class
	// HTable hTable = new HTable(con,tablename);
        //Instantiating put class
        //accepts a row name
	// Put p = new Put(Bytes.toBytes("row"));  

    	BufferedReader br = null;
    	String line = "";
    	String csvSplitBy = ",";
    	int count=0;
    	
    	try{
	    br = new BufferedReader(new FileReader(filename));
	    while ((line = br.readLine()) != null){
		//Instantiating configuration class
		Configuration con = HBaseConfiguration.create();
		//Instantiating HTable class
		HTable hTable = new HTable(con,tablename);
		//Instantiating put class
		//accepts a row name
		Put p = new Put(Bytes.toBytes(count));  
                 
		String[] row = line.split(csvSplitBy);
		//adding values using add() method
		//accepts column family name, qualifier/row name, value
		p.add(Bytes.toBytes("StoreInformation"),Bytes.toBytes("name"),Bytes.toBytes(row[0]));
		for (int i=1; i<row.length; i++) {
		    p.add(Bytes.toBytes("StoreInformation"),Bytes.toBytes("dim"+i),Bytes.toBytes(Double.parseDouble(row[1])));
		}
		//Saving the put Instance to the HTable
		hTable.put(p);
        
		count++;
		if (count%100 == 0) {
		    System.out.println("inserted "+count);
		}
		//closing HTable
		hTable.close();	
	    }
    	} catch(FileNotFoundException e){
	    e.printStackTrace();
    	}catch(IOException e){
	    e.printStackTrace();
    	}finally{
	    if(br != null){
		try{
		    br.close();
		}catch(IOException e){
		    e.printStackTrace();
		}
	    }
    	}
    	System.out.println("Done");
    }
}
