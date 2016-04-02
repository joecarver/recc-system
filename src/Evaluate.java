import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/*
 * Class used to evaluate performance of the system
 * Can compare the predicted ratings of a pseudo-randomly generated test set against their actual values
 * 	by a choice of metrics - MAE, RMSE, DifftoAvg
 *  
 */

public class Evaluate 
{
	static Connection c;
	static FileReader fw;
	
	static ArrayList<double[]> predictedTestingRatings = new ArrayList<double[]>(); //
	static ArrayList<int[]> realTestingRates = new ArrayList<int[]>();
	
	private static HashMap<Integer, HashMap<Integer, Integer>> allRatings_byTrack;
	private static HashMap<Integer, Double> averageTrackRatings;
	
	static String inputFileName = "predictTrainTest.csv";
	
	public static void main(String[] args) throws Exception{
		init();		
		
		System.out.println("RMSE: " + evaluateRMSE());
	}

	static void init() throws Exception{
		openConnection();	
		predictedTestingRatings = readPredRates(inputFileName);
		realTestingRates  = getTrainingTestData();
		
		allRatings_byTrack = getAllRatings_byTrack();

		averageTrackRatings = getAllAverageTrackRatings();
	}
	
	/*
	 * Calculate mean absolute error between predictedTestRatings and actual values for the pair
	 */
	static double evaluateMAE(){
		
		double ae = 0;
		double mae;
		if(predictedTestingRatings.size() != realTestingRates.size()){
			System.out.println("MISMATCHING LENGTHS");
			return 0;
		}
		else{
			System.out.println("SUMMING AEs...");
			for( double[] pE : predictedTestingRatings){
				
				int pos = predictedTestingRatings.indexOf(pE);
				int[] rE = realTestingRates.get(pos);
				
				double pUser = pE[0]; double pTrack = pE[1]; double pRate = pE[2];
				double rUser = rE[0]; double rTrack = rE[1]; double rRate = rE[2];
				
				if(pUser == rUser && pTrack == rTrack){
					double diff = Math.abs(pRate - rRate);
					ae += diff;
				} else {
					System.out.println("MISMATCHING PAIR at " + pos);
				}
				
			}
			mae = ae/predictedTestingRatings.size();
		}
		return mae;
	}
	
	/*
	 * Calculate Root-Mean-Square Error between predictedTestingRatings and the real ratings for these pairs
	 */
	static double evaluateRMSE(){
		
		double aeSQ = 0;
		
		if(predictedTestingRatings.size() != realTestingRates.size()){
			System.out.println("MISMATCHING LENGTHS");
			return 0;
		}else{
			for( double[] pE : predictedTestingRatings){
				int pos = predictedTestingRatings.indexOf(pE);
				int[] rE = realTestingRates.get(pos);
				
				double pUser = pE[0]; double pTrack = pE[1]; double pRate = pE[2];
				double rUser = rE[0]; double rTrack = rE[1]; double rRate = rE[2];
				
				if(pUser == rUser && pTrack == rTrack){
					double diff = pRate - rRate;
					double diffSQ = Math.pow(diff, 2);
					aeSQ += diffSQ;
				} else {
					System.out.println("MISMATCHING PAIR at " + pos);
			}
		}
		
		double mse = aeSQ/predictedTestingRatings.size();
		double rmse = Math.sqrt(mse);
		return rmse;
		}
		
	}
	
	/*
	 * Calculate difference between predicted rating for an item and item's mean for all users
	 * Not a very meaningful measure
	 */
	static double diffToAverage(){
		
		double totalDiff = 0;
		
		for (double[] predEntry : predictedTestingRatings){
			int trackID = (int) predEntry[1];
			double predRating = predEntry[2];
			if(averageTrackRatings.containsKey(trackID)){
				double avgRating = averageTrackRatings.get(trackID);
			
				double diff = Math.abs(predRating-avgRating);
				totalDiff += diff;
			}
		}
		return totalDiff/predictedTestingRatings.size();
	}
	
	/*
	 * Calculate the average (mean) rating for an item by all users that rated it
	 */
	private static HashMap<Integer, Double> getAllAverageTrackRatings() {
		System.out.println("Calculating Average Track Ratings");
		HashMap<Integer, Double> averageRatings = new HashMap<Integer, Double>();
		
		for(Integer e : allRatings_byTrack.keySet()) {
			int sum = 0;
			List<Integer> allRatings = new ArrayList<Integer>(allRatings_byTrack.get(e).values());
			for(int i : allRatings){
				sum+=i;
			}
			double mean = sum/allRatings.size();
			averageRatings.put(e, mean);
		}
		System.out.println("Done");
		return averageRatings;
	}
	
	
	/*
	 * read the predicted ratings from a specified csv
	 * Returns a 3-dimensional double array w/ structure [user, track, rating]
	 */
	private static ArrayList<double[]> readPredRates(String ratingsFileName) throws Exception {
		System.out.println("READING PREDICTED RATES");

		BufferedReader br = new BufferedReader(new FileReader(ratingsFileName));
		ArrayList<double[]> predRates = new ArrayList<double[]>();
		
		String line = "";
		String csvSplitBy = ",";
		
		while((line = br.readLine()) != null){
			String [] strs = line.split(csvSplitBy);
			double[] vals = new double[3];
			vals[0] = Double.valueOf(strs[0]);
			vals[1] = Double.valueOf(strs[1]);
			vals[2] = Double.valueOf(strs[2]);
			predRates.add(vals);
		}
		System.out.println("DONE");

		return predRates;
	}
	
	
	/*
	 * Below methods are all to open database connection/access tables
	 * For more info see other classes
	 */

	public static void openConnection() {
        try {
                Class.forName("org.sqlite.JDBC");
                c = DriverManager.getConnection("jdbc:sqlite:rating_data.db");
                c.setAutoCommit(false); 
                System.out.println("Opened database successfully");
                } catch ( Exception e ) {
              System.err.println( e.getClass().getName() + ": " + e.getMessage() );
              System.exit(0);    
         }    
	 }
	
	private static ArrayList<int[]> getTrainingTestData() {
		ArrayList<int[]> tmpTrainTest = new ArrayList<int[]>();
		
		try {
			System.out.println("Loading test data");
			Statement stat = c.createStatement();
			ResultSet rs = stat.executeQuery("SELECT userID, trackID, rating from trainingData WHERE rowID % 100 = 0 ORDER BY userID ASC;");
			
			while(rs.next()){
				int[] data = new int[3];
				data[0] = rs.getInt("userID");
				data[1] = rs.getInt("trackID");
				data[2] = rs.getInt("rating");
				tmpTrainTest.add(data);
			}
			rs.close();
			stat.close();
			System.out.println("Done");
			
		 }	catch(Exception e){
	         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			 System.exit(0);
		 }
		return tmpTrainTest;
	}
	
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
}
