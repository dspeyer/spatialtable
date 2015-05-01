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
     
    public static void main(String[] args) throws IOException{
		//Instantiating configuration class
		Configuration con = HBaseConfiguration.create();
		//Instantiating HBaseAdmin class
		HBaseAdmin admin = new HBaseAdmin(con);
	        //Instantiating table descriptor class
	 	HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("Starbuck"));
		//Adding column families to table descriptor
		tableDescriptor.addFamily(new HColumnDescriptor("StoreInformation"));
		CreateTable obj = new CreateTable();
		obj.run();
                //Execute the table through admin
                admin.createTable(tableDescriptor);
                System.out.println("Table created");
	}
	
    public void run() throws IOException{
        //Instantiating configuration class
       // Configuration con = HBaseConfiguration.create();
        //Instantiating HTable class
       // HTable hTable = new HTable(con,"Starbuck");
        //Instantiating put class
        //accepts a row name
       // Put p = new Put(Bytes.toBytes("row"));  

    	String csvFile = "/home/hduser/peiran-scratch/starbucks.csv";
    	BufferedReader br = null;
    	String line = "";
    	String csvSplitBy = ",";
    	int count=0;
    	
    	try{
    		br = new BufferedReader(new FileReader(csvFile));
    		while ((line = br.readLine()) != null){
    			 //Instantiating configuration class
                         Configuration con = HBaseConfiguration.create();
                         //Instantiating HTable class
                         HTable hTable = new HTable(con,"Starbuck");
                         //Instantiating put class
                         //accepts a row name
                         Put p = new Put(Bytes.toBytes(count));  
                 
                        String[] row = line.split(csvSplitBy);
                        String table[][] = {row};
                        //adding values using add() method
                        //accepts column family name, qualifier/row name, value
                        p.add(Bytes.toBytes("StoreInformation"),Bytes.toBytes("name"),Bytes.toBytes(table[0][0]));
                        p.add(Bytes.toBytes("StoreInformation"),Bytes.toBytes("latitude"),Bytes.toBytes(table[0][1]));
                        p.add(Bytes.toBytes("StoreInformation"),Bytes.toBytes("longitude"),Bytes.toBytes(table[0][2]));
                        //Saving the put Instance to the HTable
                        hTable.put(p);
        
    	                System.out.println("name= "+ table[0][0]+",lat="+table[0][1]+",longi="+table[0][2]);
    	                System.out.println(count);
                        count++;
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
