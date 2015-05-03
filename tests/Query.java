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
import java.util.*;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedList;


public class Query{
    public static void main( String args[] ) throws UnknownHostException, FileNotFoundException, IOException{

                // To connect to mongodb server
                MongoClient mongoClient = new MongoClient( "localhost" , 27017 ); 
                // Now connect to your databases
                DB db = mongoClient.getDB( "myDB" );
                System.out.println("Connect to database successfully");
                
                DBCollection collection = db.getCollection(args[1]); 
                System.out.println("Collection "+args[1]+"selected");

                
		BufferedReader br = new BufferedReader(new FileReader(args[0]));
		String line;
		while ((line = br.readLine()) != null){
		    //System.out.println("query: "+line);
		    String[] row = line.split(",");
		    double x1 = Double.parseDouble(row[0]);
		    double x2 = Double.parseDouble(row[1]);
		    double y1 = Double.parseDouble(row[2]);
		    double y2 = Double.parseDouble(row[3]);
		    double area = (x2-x1)*(y2-y1);

		    long startTime = System.nanoTime();
		    //LinkedList<double[]> box = new LinkedList<double[]>();
		    // Set the lower left point
		    //box.addLast(new double[] {  x1, y1 });
		    // Set the upper right point
		    //box.addLast(new double[] { x2, y2 });

		    double[][] box = { { x1, y1 }, { x2, y2 }};

		    //		    System.out.println("making query");
		    BasicDBObject query = new BasicDBObject("loc", new BasicDBObject("$geoWithin", new BasicDBObject("$box", box)));
		    //System.out.println("getting cursor");
		    DBCursor cursor = collection.find(query);
		    //System.out.println("got cursor");
		    int cnt=0;
		    while (cursor.hasNext()) {
			cursor.next();
			cnt++;
		    }
		    long endTime = System.nanoTime();
		    long responseTime = (endTime -startTime);
		    System.out.println(area+","+cnt+","+(responseTime/1e6));
		}
	}
}
