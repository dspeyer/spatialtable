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
                
                
                DBCollection coll = db.getCollection("Starbuck"); 
                System.out.println("Collection mycoll selected successfully"); 
                
            
                br = new BufferedReader(new FileReader(args[0]));
                while ((line = br.readLine()) != null){
		    String[] row = line.split(csvSplitBy);
		    
		    BasicDBObject doc = new BasicDBObject("name", (row[0]));
		    for (int i=1; i<row.length; i++) {
			doc.append("dim"+i, Double.parseDouble(row[i]));
		    }
                                    
		    coll.insert(doc);
		    count++;
		    if (count % 1000 == 0) {
			System.out.println("inserted "+count);
		    }

		}
                
            }catch(Exception e){
                System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            } 
        }
}
