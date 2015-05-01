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
            String csvFile = "/home/hduser/peiran-scratch/starbucks.csv";
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
                
            
                br = new BufferedReader(new FileReader(csvFile));
                while ((line = br.readLine()) != null){
                         String[] row = line.split(csvSplitBy);
                         String table[][] = {row};
                         

                         BasicDBObject doc = new BasicDBObject("name", (table[0][0])).
                                    append("latitude", Double.parseDouble(table[0][1])).
                                    append("longitude", Double.parseDouble(table[0][2]));
                                    
                        coll.insert(doc);
                        System.out.println("Document inserted successfully");
                        System.out.println("name= "+ table[0][0]+",lat="+table[0][1]+",longi="+table[0][2]);
                        System.out.println(count);
                        count++;
                     }
                
            }catch(Exception e){
                System.err.println( e.getClass().getName() + ": " + e.getMessage() );
            } 
        }
}
