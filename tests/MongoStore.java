import com.mongodb.MongoException;
import com.mongodb.WriteConcern; 
import com.mongodb.DB;
import com.mongodb.DBCollection; 
import com.mongodb.BasicDBObject; 
import com.mongodb.DBObject; 
import com.mongodb.DBCursor; 
import com.mongodb.ServerAddress;
import com.mongodb.MongoClient;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.util.Arrays;

public class MongoStore{
        public static void main( String args[] ){
            BufferedReader br = null;
            String line = "";
            String csvSplitBy = ",";
            int count=0;

            try{
                // To connect to mongodb server
                MongoClient mongoClient = new MongoClient( "localhost" , 27017 ); 
                // Now connect to your databases
                DB db = mongoClient.getDB( "myDB" );
                System.out.println("Connect to database successfully");
                
                String[] tableName = args[0].split("\\."); 
                System.out.println("args[0]="+args[0]+" tabletName[0]="+tableName[0]);
                DBCollection coll = db.getCollection(tableName[0]); 
                System.out.println("Collection "+tableName[0]+ "  selected successfully"); 
            
                br = new BufferedReader(new FileReader(args[0]));
		double min=9999, max=-9999;
                while ((line = br.readLine()) != null){
		    String[] row = line.split(csvSplitBy);
		    
		    BasicDBObject doc = new BasicDBObject("name", (row[0]));
                    double[] coord = {Double.parseDouble(row[2]), Double.parseDouble(row[1])};
		    if (coord[0]<min) min=coord[0];
		    if (coord[0]>max) max=coord[0];
		    if (coord[1]<min) min=coord[1];
		    if (coord[1]>max) max=coord[1];

                    doc.append("loc",coord);
                   // System.out.println("loc: "+row[2]+" "+ row[1]);
		    //for (int i=1; i<row.length; i++) {
		    //   	doc.append("dim"+i, Double.parseDouble(row[i]));
		    //}
                                    
		    coll.insert(doc);
		    count++;
		    if (count % 1000 == 0) {
			System.out.println("inserted "+count);
		    }
		}
		BasicDBObject params = new BasicDBObject();
		params.append("min",min);
		params.append("max",max);
		coll.createIndex(new BasicDBObject("loc","2d"), params);

            }catch(Exception e){
                System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            }
        }
}
