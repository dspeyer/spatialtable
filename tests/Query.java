import com.mongodb.MongoException;
import com.mongodb.WriteConcern; 
import com.mongodb.DB;
import com.mongodb.DBCollection; 
import com.mongodb.BasicDBObject; 
import com.mongodb.DBObject; 
import com.mongodb.DBCursor; 
import com.mongodb.ServerAddress;
import com.mongodb.MongoClient;
import com.mongodb.Cursor;

import java.net.UnknownHostException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.io.IOException;
import java.util.*;
import java.util.Arrays;

public class Query{
        public static void main( String args[] ) throws UnknownHostException{

                // To connect to mongodb server
                MongoClient mongoClient = new MongoClient( "localhost" , 27017 ); 
                // Now connect to your databases
                DB db = mongoClient.getDB( "myDB" );
                System.out.println("Connect to database successfully");
                
                DBCollection collection = db.getCollection("Starbuck"); 
                System.out.println("Collection mycoll selected successfully"); 

                
                long startTime = System.nanoTime();
                for(int i=1; i<=10;i++){
                rangequery(collection);
                
                    }

                long endTime = System.nanoTime();
                long responseTime = (endTime -startTime)/10;
                System.out.println(responseTime);  //nano seconds

            }

        private static void rangequery(DBCollection collection){
                Random rand = new Random();
                int random1 = rand.nextInt(45)+1;
                int random2 = rand.nextInt(45)+45;
                int random3 = rand.nextInt(90)+1;
                int random4 = rand.nextInt(90)+90;
                BasicDBObject query = new BasicDBObject();
                query.put("latitude", new BasicDBObject("$gt",random1).append("$lte", random2));
                query.put("longitude", new BasicDBObject("$gt",random3).append("$lte", random4));
                //BasicDBObject query = new BasicDBObject.append("latitude", new BasicDBObject("$lte", 50));
                

                DBCursor cursor = collection.find(query);
                     while (cursor.hasNext()) {
                      System.out.println(cursor.next());
                       }
          }

}
