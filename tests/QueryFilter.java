import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration; 
import org.apache.hadoop.hbase.client.HTable; 
import org.apache.hadoop.hbase.client.Result; 
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan; 
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
//import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;


public class QueryFilter{
    public static String filename;
    public static String tablename;
    public static void main(String args[]) throws IOException{
	filename=args[0];
	tablename=args[1];
	// Instantiating Configuration class
        Configuration con = HBaseConfiguration.create();
        // Instantiating HTable class
        HTable table = new HTable(con, tablename);
        // Instantiating the Scan class 
        
        Random rand = new Random();
	BufferedReader br = new BufferedReader(new FileReader(filename));
	String line;
	while ((line = br.readLine()) != null){
	    String[] row = line.split(",");
	    Scan scan = new Scan();
	    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
	    // Scanning the required columns 
	    scan.addColumn(Bytes.toBytes("StoreInformation"), Bytes.toBytes("name"));
	    double vol = 1;
	    for (int i=0; i<row.length/2; i++) {
		String col="dim"+(i+1);
		double start = Double.parseDouble(row[2*i]);
		double end = Double.parseDouble(row[2*i+1]);
		vol *= end - start;
		scan.addColumn(Bytes.toBytes("StoreInformation"), Bytes.toBytes(col));
		SingleColumnValueFilter ge = new SingleColumnValueFilter(Bytes.toBytes("StoreInformation"),
									 Bytes.toBytes(col),
									 CompareOp.GREATER_OR_EQUAL,
									 Bytes.toBytes(start));
		filterList.addFilter(ge);
		SingleColumnValueFilter le = new SingleColumnValueFilter(Bytes.toBytes("StoreInformation"),
									 Bytes.toBytes(col),
									 CompareOp.LESS_OR_EQUAL,
									 Bytes.toBytes(end));
		filterList.addFilter(le);
	    }
	    scan.setFilter(filterList);
	    long startTime = System.nanoTime();
	    ResultScanner scanner1 = table.getScanner(scan); 
	    int cnt=0;
	    for (Result res : scanner1) { 
		cnt++;
	    }
	    scanner1.close();
	    long endTime = System.nanoTime();
	    long responseTime = endTime-startTime;
	    System.out.println(vol+", "+cnt+", "+(responseTime/1e6));   
	}
    }
}
