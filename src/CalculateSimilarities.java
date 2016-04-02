import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/*
 * Calculate all similarities between a set of vectors in a dataset
 * This class is designed to deal with one vector for each item - containing user ratings for that track
 * Connection c - communicates with SQLite database
 */

public class CalculateSimilarities 
{
	static Connection c = null;
	
	public static HashMap<Integer, HashMap<Integer, Integer>> allRatings_byTrack;
	public static String databaseName = "rating_data.db";
	
	public static void main(String[] args)
	{
		openConnection(databaseName);
		allRatings_byTrack = getAllRatings_byTrack();
		
		itemSimilarities();
	}
	
	/*
	 * Loops over every track and compare it to every other track using 'sim_cosine'
	 * If similarity shows positive correlation (over 0), the value is saved with the two track IDs
	 * The database is closed and re-opened every time 250/~30,000 tracks have been processed to reduce memory leak
	 * 		It is vital to commit before doing this so no values are lost
	 * 
	 */
	public static void itemSimilarities()
	{
		long counter = 1;

		for( Entry<Integer, HashMap<Integer, Integer>> track1 : allRatings_byTrack.entrySet())
		{
			for ( Entry<Integer, HashMap<Integer, Integer>> track2 : allRatings_byTrack.entrySet())
			{
				double similarity = sim_cosine(track1.getValue(), track2.getValue());
				
				if(similarity>0){
					saveSimilarity(track1.getKey(), track2.getKey(), similarity, "itemSimilaritiesTester", counter);
					counter++;
				}
			}
			if(track1.getKey() % 250 == 0){
				try{
					c.commit();
					System.out.println("Closing database");
					c.close();
					System.out.println("Closed database successfully"); 
					openConnection(databaseName);
				} catch(Exception e){ e.getMessage(); }
				    
			}
			System.out.println("Processed " + track1.getKey());
		}
	}
	
	/*
	 * Is called by itemSimilarities for each pair of tracks with sim>0. 
	 * tableName parameter is purely to make code more portable in the future
	 * counter parameter keeps track of how many values have been put in the cache (managed by Connection c)
	 * 		we commit values to the database after each batch of 750,000 is processed
	 * 		autocommit would commit each value separately and take a v long time!
	 */
	public static void saveSimilarity(int track1, int track2, double similarity, String tableName, long counter)
	{
		try{
			c.setAutoCommit(false);
	        PreparedStatement stat = c.prepareStatement("INSERT into " +tableName+ " VALUES (?,?,?)");
	       
	        stat.setInt(1, track1);
	        stat.setInt(2, track2);
	        stat.setDouble(3, similarity);
	        stat.execute();

	        if ((counter) % 750000 == 0) {
	            c.commit();
	            System.out.println("Committed Data");
	        }    
	        } catch ( Exception e ) {
	          System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	          System.exit(0);
	        }
	}
	
	/*
	 * Uses retainAll method to return intersection of two sets
	 * In this case, all users who rated track 1 and track 2
	 */
	public static Set<Integer> getMutualEntries(HashMap<Integer, Integer> set1, HashMap<Integer, Integer> set2){
		Set<Integer> mutuals = new HashSet<Integer>(set1.keySet());
		mutuals.retainAll(set2.keySet());
		
		return mutuals;
	}
	
	
	/*
	 * Calculates the similarity between two vectors using pearson similarity
	 * Adapted from 'Programming Collective Intelligence (Toby Segaran)' p13
	 */
	public static double sim_cosine(HashMap<Integer, Integer> set1Ratings, HashMap<Integer, Integer> set2Ratings){
		Set<Integer> mutualEntries = getMutualEntries(set1Ratings, set2Ratings);
		
		if(mutualEntries.isEmpty()){
			//System.out.println("NO MUTUAL TRACKS");
			return 0;
		}
		
		double ratingE1, ratingE2;
		
		double sum1 = 0, sum2 = 0;
		double sum1sq = 0, sum2sq = 0, sum12 = 0;
		double mutualCount = mutualEntries.size();
		
		//add up all preferences for each entry, their squares, and the product of the two tracks
		for(int entryID : mutualEntries){	
			ratingE1 = set1Ratings.get(entryID);
			ratingE2 = set2Ratings.get(entryID);
			
			sum1 += ratingE1;
			sum2 += ratingE2;
			
			sum1sq += Math.pow(ratingE1, 2);
			sum2sq += Math.pow(ratingE2, 2);
			
			sum12 += ratingE1 * ratingE2;
		}
		
		double numerator = sum12-(sum1*sum2/mutualCount);
		double denominator = Math.sqrt((sum1sq - Math.pow(sum1, 2)/mutualCount) * (sum2sq - Math.pow(sum2, 2)/mutualCount));
		mutualEntries.clear();
		
		if (denominator == 0) return 0;
		return numerator/denominator;
	}

	/*
	 * Open connection to existing database - c
	 * PARAM - databaseName should be in format myinfo.db
	 */
	 public static void openConnection(String databaseName) {
         try {
                 Class.forName("org.sqlite.JDBC");
                 c = DriverManager.getConnection("jdbc:sqlite:" + databaseName);
                 c.setAutoCommit(false); 
                 System.out.println("Opened database successfully");
                 } catch ( Exception e ) {
               System.err.println( e.getClass().getName() + ": " + e.getMessage() );
               System.exit(0);    
          }    
	 }
	 /*
	  * Delete an existing table from SQL database connected to c
	  * PARAM - tableName must be an existing table
	  */
	 public static void deleteTable(String tableName){
			try {
				System.out.println("Deleting Table");
				Statement stmt = c.createStatement();
				String sql = ("DROP table " + tableName);
				stmt.executeUpdate(sql);
				c.commit();
				stmt.close();
				System.out.println("Done");
			} catch(Exception e) {
				e.printStackTrace();
			}
	}
	 
	 /*
	  * Create a new table in SQL database connected to c
	  * PARAM - sqlQuery in format: "CREATE TABLE tablename (FieldName TYPE, FieldName TYPE, FieldName TYPE)"

	  */
	 public static void createTable(String sqlQuery) {
			try {
				System.out.println("Creating Table");
				Statement stmt = c.createStatement();
				stmt.executeUpdate(sqlQuery);
				c.commit();
				stmt.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	 
	 /*
	  * Read all training data from database and store it in a nested HashMap structure, indexed by track
	  */
	 public static HashMap<Integer, HashMap<Integer, Integer>> getAllRatings_byTrack()
	 {
		 HashMap<Integer, HashMap<Integer, Integer>> allRatings_byTrack = new HashMap<Integer, HashMap<Integer, Integer>>();
		 
		 try {
				System.out.println("Loading ratings into track-centric hashmap");
				Statement stat = c.createStatement();
				ResultSet rs = stat.executeQuery("SELECT * from trainingData");
				
				while(rs.next()){
					int userID = rs.getInt("userID");
					int trackID = rs.getInt("trackID");
					int rating = rs.getInt("rating");
					
					if(allRatings_byTrack.containsKey(trackID)){
						HashMap<Integer, Integer> userRatings = allRatings_byTrack.get(trackID);
						userRatings.put(userID, rating);
						allRatings_byTrack.put(trackID, userRatings);
					} else{
						HashMap<Integer, Integer> userRatings = new HashMap<Integer, Integer>();
						userRatings.put(userID, rating);
						allRatings_byTrack.put(trackID, userRatings);
					}
				}
				rs.close();
				stat.close();
				System.out.println("Done");
				
		 }	catch(Exception e){
             System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			 System.exit(0);
		 }
		 
		 return allRatings_byTrack;
	 }
}
