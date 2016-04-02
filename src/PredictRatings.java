import java.sql.*;
import java.util.*;
import java.util.Map.Entry;
import java.io.*;
public class PredictRatings 
{
	static Connection c;
	static ArrayList<int[]> testData = new ArrayList<int[]>();
	private static HashMap<Integer, HashMap<Integer, Integer>> allRatings_byTrack;
	private static HashMap<Integer, HashMap<Integer, Integer>> allRatings_byUser;
	static FileWriter fw;
	
	//csv file to save predictions to
	static String saveLocation = "predictTrainTest.csv";
	public static String databaseName = "rating_data.db";

	public static void main(String[] args) {
		init();
		
		predictRatings();
	}
	
	/*
	 * Carry out set-up steps 
	 * Open connection, get and restructure all the data,
	 * Configure the filewriter
	 */
	static void init() {
		openConnection(databaseName);
		testData = getTestData();
		
		allRatings_byTrack = getAllRatings_byTrack();
		allRatings_byUser = getAllRatings_byUser();
		fw = null;
		
		try {
			fw = new FileWriter(saveLocation);
		} catch (IOException e1) { e1.printStackTrace(); }
	}
	
	/*
	 * For every value we have to predict, 
	 * 		find other tracks that this user has rated, get their similarities to this track
	 * 			Sum the similarities, and the similarity*rating product
	 * 			Divide to get final rating
	 * 		if all other tracks rated by this user have no similarity with this track, we need an alternative prediction function
	 * 
	 *  FileWriter fw writes all values to a csv for analysis & submission
	 */
	private static void predictRatings() {
		//For every value we have to predict
		for(int[] entry : testData) {
			int uID = entry[0];
			int tID = entry[1];
			
			//System.out.println("Predicting " + uID + " -- " + tID);
			HashMap<Integer, Double> similarities = getSimilaritiesForTrack(tID);
			
			double similaritySum = 0;
			double simTrackSum = 0;
			double predictedRating;
			
			//find other tracks the user has rated, get their similarity to this track
			if(allRatings_byUser.containsKey(uID)){
				for(Entry<Integer, Integer> e : allRatings_byUser.get(uID).entrySet()){
					int trackID = e.getKey();
					int rating = e.getValue();
					
					if(trackID != tID){
						if(similarities.containsKey(trackID)){
							double sim = similarities.get(trackID);
							similaritySum += sim;
							simTrackSum += (sim*rating);
						} 			
					}	
				}

				//if all other tracks rated by this user have no similarity with this track, we need an alternative prediction function
				if(similaritySum == 0){
					predictedRating = alternativePrediction(uID, tID);
				} else {
					predictedRating = (simTrackSum/similaritySum);
				}
				
				System.out.println("Predicted user " + uID +  " would rate track " + tID + " --- " + predictedRating);
				
				try{
					fw.append(String.valueOf(uID));
					fw.append(",");
					fw.append(String.valueOf(tID));
					fw.append(",");
					fw.append(String.valueOf(predictedRating));
					fw.append("\n");
				} catch (IOException e){
					e.printStackTrace();
				}
			}
			try {
				fw.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("DONE");
		try{ fw.close(); } catch (IOException e) { e.printStackTrace(); }
	}



	/*
	 * In case of item sparsity, we need a user based-prediction function
	 * Uses pearson similarity, then works identical to standard prediction function
	 */
	private static double alternativePrediction(int uID, int tID) {
		double predictedRating;
		double similaritySum = 0;
		double simTrackSum = 0;
		
		if(allRatings_byTrack.containsKey(tID)){
			for(Entry<Integer, Integer> e : allRatings_byTrack.get(tID).entrySet()) {
				int userID = e.getKey();
				int rating = e.getValue();
				if(userID != uID) {
					double sim = sim_pearson(allRatings_byUser.get(uID), allRatings_byUser.get(userID));
					if(sim>0){
						similaritySum += sim;
						simTrackSum += (sim*rating);
					}
				}
			}
		}
		if(similaritySum == 0){
			predictedRating = 0;
		} else {
			predictedRating = simTrackSum/similaritySum;
		}
		return predictedRating;
	}

	/*
	 * Pearson similarity adapted from 'Programming Collective Intelligence p13'
	 */
	public static double sim_pearson(HashMap<Integer, Integer> user1Ratings, HashMap<Integer, Integer> user2Ratings){
		Set<Integer> mutualEntries = getMutualEntries(user1Ratings, user2Ratings);
		
		if(mutualEntries.isEmpty()){
			//System.out.println("NO MUTUAL TRACKS");
			return 0;
		}
		
		double ratingE1, ratingE2;
		
		double sum1 = 0, sum2 = 0;
		double sum1sq = 0, sum2sq = 0, sum12 = 0;
		double mutualCount = mutualEntries.size();
		
		//add up all preferences for each entry, their squares, and the product of the two users ratings
		for(int entryID : mutualEntries){	
			ratingE1 = user1Ratings.get(entryID);
			ratingE2 = user2Ratings.get(entryID);
			
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
	 * Uses retainAll method to return intersection of two sets
	 * In this case, all tracks rated by user 1 and user 2
	 */
	public static Set<Integer> getMutualEntries(HashMap<Integer, Integer> set1, HashMap<Integer, Integer> set2){
		if(set1 == null || set2 == null){
			return new HashSet<Integer>();
		}
		else{
			Set<Integer> mutuals = new HashSet<Integer>(set1.keySet());
			mutuals.retainAll(set2.keySet());
		
			return mutuals;
		}
	}

	 /*
	  * Read all training data from database and store it in a nested HashMap structure, indexed by user
	  */
	private static HashMap<Integer, HashMap<Integer, Integer>> getAllRatings_byUser() {
		HashMap<Integer, HashMap<Integer, Integer>> allRatings_byUser = new HashMap<Integer, HashMap<Integer, Integer>>();
		 
		 try {
				System.out.println("Loading ratings into user-centric map");
				Statement stat = c.createStatement();
				ResultSet rs = stat.executeQuery("SELECT * from trainingData");
				
				while(rs.next()){
					int userID = rs.getInt("userID");
					int trackID = rs.getInt("trackID");
					int rating = rs.getInt("rating");
					
					if(allRatings_byUser.containsKey(userID)){
						HashMap<Integer, Integer> trackRatings = allRatings_byUser.get(userID);
						trackRatings.put(trackID, rating);
						allRatings_byUser.put(userID, trackRatings);
					} else{
						HashMap<Integer, Integer> trackRatings = new HashMap<Integer, Integer>();
						trackRatings.put(trackID, rating);
						allRatings_byUser.put(userID, trackRatings);
					}
				}
				rs.close();
				stat.close();
				System.out.println("Done");
				
		 }	catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			 System.exit(0);
		 }
		 
		 return allRatings_byUser;
	}
	
	 /*
	  * Read all training data from database and store it in a nested HashMap structure, indexed by track
	  */
	private static HashMap<Integer, HashMap<Integer, Integer>> getAllRatings_byTrack() {
		
		HashMap<Integer, HashMap<Integer, Integer>> allRatings_byTrack = new HashMap<Integer, HashMap<Integer, Integer>>();
		 
		 try {
				System.out.println("Loading ratings into track-centric map");
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
						userRatings.put(trackID, rating);
						allRatings_byTrack.put(userID, userRatings);
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

	/*
	 * Load all similarities for given trackID into a map - <track, similarity> pairs
	 * Nice and quick as database is indexed by track1ID
	 */
	public static HashMap<Integer, Double> getSimilaritiesForTrack(int trackID)
	{
		HashMap<Integer, Double> similarities = new HashMap<Integer, Double>();
		//System.out.println("Getting similarities for track " + trackID);
		try{
			
			Statement stat = c.createStatement();
			ResultSet rs = stat.executeQuery("SELECT * from itemSimilarities WHERE Item1=" + trackID);
			while(rs.next()){
				similarities.put(rs.getInt("Item2"), rs.getDouble("Similarity"));
			}
		 } catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			 System.exit(0);
		 }
		//System.out.println("DONE");
		return similarities;
	}
	
	/*
	 * Load all data to be predicted
	 * Returned as a list of [user, track] int arrays
	 */
	public static ArrayList<int[]> getTestData(){
		ArrayList<int[]> tmpTest = new ArrayList<int[]>();
		
		try {
			System.out.println("Loading test data");
			Statement stat = c.createStatement();
			ResultSet rs = stat.executeQuery("SELECT * from testData ORDER BY userID ASC;");
			
			while(rs.next()){
				int[] data = new int[2];
				data[0] = rs.getInt("userID");
				data[1] = rs.getInt("trackID");
				tmpTest.add(data);
			}
			rs.close();
			stat.close();
			System.out.println("Done");
			
		 }	catch(Exception e){
	         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			 System.exit(0);
		 }
		return tmpTest;
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
}
