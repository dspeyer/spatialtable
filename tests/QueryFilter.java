import java.io.IOException;
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
public static void main(String args[]) throws IOException{
         // Instantiating Configuration class
        Configuration con = HBaseConfiguration.create();
        // Instantiating HTable class
        HTable table = new HTable(con, "Starbuck");
        // Instantiating the Scan class 
        
        Random rand = new Random();
        //float random1 = rand.nextFloat(90)+1;
long startTime = System.nanoTime();
 for(int i=1;i<=5;i++){       
         //int random1 = rand.nextInt(45)+45;
        int random2 = rand.nextInt(90)+1;
       // int random3 = rand.nextInt(90)+1;
        int random4 = rand.nextInt(180)+1;
 
        Scan scan = new Scan();
        // Scanning the required columns 
        scan.addColumn(Bytes.toBytes("StoreInformation"), Bytes.toBytes("name"));
        scan.addColumn(Bytes.toBytes("StoreInformation"), Bytes.toBytes("latitude"));
        scan.addColumn(Bytes.toBytes("StoreInformation"), Bytes.toBytes("longitude"));
        

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);//MUST_PASS_ONE
        SingleColumnValueFilter a = new SingleColumnValueFilter(Bytes.toBytes("StoreInformation"),
        Bytes.toBytes("latitude"), CompareOp.LESS_OR_EQUAL, Bytes.toBytes("random2"));
        filterList.addFilter(a);
/*       
      SingleColumnValueFilter b = new SingleColumnValueFilter(Bytes.toBytes("StoreInformation"),
        Bytes.toBytes("latitude"), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("1"));
        filterList.addFilter(b);
*/
        SingleColumnValueFilter c = new SingleColumnValueFilter(Bytes.toBytes("StoreInformation"),
        Bytes.toBytes("longitude"), CompareOp.LESS_OR_EQUAL, Bytes.toBytes("random4"));
        filterList.addFilter(c);
/* 
        SingleColumnValueFilter d = new SingleColumnValueFilter(Bytes.toBytes("StoreInformation"),
        Bytes.toBytes("longitude"), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes("0"));
        filterList.addFilter(d);
*/
//        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,a,b,c,d);//MUST_PASS_ONE
        scan.setFilter(filterList);
        ResultScanner scanner1 = table.getScanner(scan); 
           for (Result res : scanner1) { 
             System.out.println(res); 
             }
        scanner1.close();
    
        System.out.println("done");
}  
 long endTime = System.nanoTime();
      long responseTime = (endTime -startTime)/5;
     System.out.println(responseTime);   
}
}
