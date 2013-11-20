
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.StringTokenizer;
import java.util.*;
import java.sql.*;

public class adwords {

	Connection conn =null;
		    
	// Create a Statement
	PreparedStatement pstmt = null;
	
	int K = 5;
	//int qid = 77;
	//int qid = 79;
	int qid = 97;

	HashMap<Keyword,Double> ranks = new HashMap<Keyword,Double>();
	
	class Ranker implements Comparator<Keyword> {
		Map<Keyword, Double> base;
		public Ranker (Map<Keyword, Double> base) {
			this.base = base;
		}

		public int compare(Keyword a, Keyword b) {
			// break the tie
			if (base.get(a) == base.get(b)) {
				return (a.advertiserid <= b.advertiserid)? -1:1;
			}
			return (base.get(a) > base.get(b))? -1: 1;
		}
	}

	private class Keyword {
		int advertiserid;
		String keyword;
		double bid;
		private Keyword(int aid, String keyword, double bid) {
			advertiserid = aid;
			this.keyword = keyword;
			this.bid = bid;
		}
	}

	/**
	 * @param args
	 */
	  public static void main (String args [])
		  {
			adwords aw = new adwords();
			try{
			aw.setupDB();
			} catch(Exception e) {
				e.printStackTrace();
			}
			System.out.println("DB setup finished.");

			aw.search();
			//aW.rank();
			aw.charge();
			aw.closeDB();
		}

	private void search() {
		String query0 = "select * from queries where qid = ?";
		String query = "select * from keywords where keyword like '%?%'";
		query = "select * from keywords where ? like '%'|| keyword || '%'";
		//query = "select advertiserid,keyword,bid from keywords,advertisers where ? like '%'|| keyword || '%' and ";
		String query2 = "http www.flickr.com photos 88145967 n00 24368586 in pool-32148876 n00";

		try {
			pstmt = conn.prepareStatement(query0);
			pstmt.setInt(1, qid);
				
			ResultSet rs = pstmt.executeQuery();
			//System.out.println("query executed");
			while (rs.next()) {
				qid = rs.getInt("qid");
				query2 = rs.getString("query");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	
		System.out.println(query);
		System.out.println(query2);

	    	StringTokenizer tmp = new StringTokenizer(query2);
		HashMap tokens = new HashMap();
		String tok;	
		int cnt;
	    	while(tmp.hasMoreElements()) {
			tok = tmp.nextToken();
			//System.out.print (tok + ", " );
			cnt = tokens.containsKey(tok) ? ((Integer)tokens.get(tok) + 1) : 1;
			tokens.put(tok, cnt);
		}
		System.out.println(tokens);

		int n = 0;

		try {
			pstmt = conn.prepareStatement(query);
			pstmt.setString(1, query2);
				
			ResultSet rs = pstmt.executeQuery();
			//System.out.println("query executed");
			System.out.println(rs);
 
			int aid;
			String keyword;
			double bid, ctc;
			double score, adrank;

			System.out.println( "advertiserid keyword bid");
			while (rs.next()) {
 
				aid = rs.getInt("advertiserid");
				keyword = rs.getString("keyword");
				bid = rs.getDouble("bid");
				
				if(tokens.containsKey(keyword)) {
					System.out.println((++n) + " " + aid  + " " + keyword + " " + bid );
 
					ctc = getCTC(aid);
					score = similarity(tokens, aid, keyword);
					adrank = bid * ctc * score;
					Keyword k = new Keyword(aid, keyword, bid);
					ranks.put(k, adrank);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

		// AdRank = bid*ctc*similarity
		// select keywords.bid*advertisers.ctc*(select count(*) from queries) as adrank from keywords,advertisers where keywords.keyword = 'photos' and keywords.advertiserid = 878 and advertisers.advertiserid = keywords.advertiserid order by adrank;
		// select keywords.bid*advertisers.ctc as adrank from keywords,advertisers where keywords.keyword = 'photos' and keywords.advertiserid = 878 and advertisers.advertiserid = keywords.advertiserid order by adrank;

	private void rank() {
		TreeMap<Keyword,Double> map = new TreeMap(new Ranker(ranks));
		map.putAll(ranks);	
		System.out.println(map);	
	}

	private void charge() {
		// only for first 100*x% ctc impressions
		// repeat every 100 impressions
		//rank();
		TreeMap<Keyword,Double> map = new TreeMap(new Ranker(ranks));
		map.putAll(ranks);	
		System.out.println(map);	

		//System.out.println(ranks);	

		double balance = 0, budget = 0;
		int impression = 0;
		int i = 1;
		for(Map.Entry<Keyword,Double> entry: map.entrySet()) {	
			if (i > K) break;
			Keyword key = (Keyword)entry.getKey();
			Double score = (Double)entry.getValue();
//			System.out.println(i + " " + key.advertiserid + " " + key.keyword + " " + score);
			
			// get balance and impression from db
			String query = "select * from advertisers where advertiserid = ?";
			int aid = key.advertiserid;
	
			try{
				pstmt = conn.prepareStatement(query);	
				pstmt.setInt(1, aid);
				ResultSet rs = pstmt.executeQuery();
				while(rs.next()) {
					budget = rs.getDouble("budget");
					//balance = rs.getDouble("balance");
					//impression = rs.getDouble("impression");
					//System.out.println(rs.getInt("advertiserid") + " " + ctc);
				}
				System.out.println();						
			} catch (Exception e) {}
	

			// charge only if balance < budget - bid
			// and impression <= x*100
			//if(adver.balance > 
			balance = budget - key.bid;
			//balance = balance - key.bid;
			if (impression == 100) impression = 0;
			String q = "update advertisers balance = ? and impression = ?";
			// update db: budget and impression. if impression is 100, reset to zero
			/*
			try{
				pstmt = conn.prepareStatement(q);	
				pstmt.setInt(1, advertiserid);
				pstmt.executeUpdate();
			} catch (Exception e) {}
			*/
			
			// write to final report output file
			// qid, rank, advertiserid, balance, budget	
			// qid, i, key.advertiserid, advertiser.balance, advertiser.budget 
			System.out.println(qid + " " + i + " " + aid + " " + balance + " " + budget);

			i ++;
		}
	}
		
	private double getCTC(int advertiserid) {
		String query = "select * from advertisers where advertiserid = ?";
		double ctc = 0;
	
		try{
			pstmt = conn.prepareStatement(query);	
			pstmt.setInt(1, advertiserid);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				ctc = rs.getDouble("ctc");
				System.out.println(rs.getInt("advertiserid") + " " + ctc);
			}
			System.out.println();						
		} catch (Exception e) {}
		return ctc;
	}
	
	private double similarity(HashMap tokens, int aid, String keyword) {
		double score = 0;
		System.out.println(aid + " " + keyword);

		String query2 = "select * from keywords where keywords.advertiserid = ?";

		try {
			HashMap keywords = new HashMap();
			int cnt; String k;

			pstmt = conn.prepareStatement(query2);	
			pstmt.setInt(1, aid);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				k = rs.getString("keyword");
				System.out.println(rs.getInt("advertiserid") + " " + k);
				cnt = keywords.containsKey(k) ? ((Integer)keywords.get(k) + 1) : 1;
				keywords.put(k, cnt);
			}
			System.out.println(keywords);
						
			HashMap tokFreqs = new HashMap(tokens);
			HashMap keyFreqs = new HashMap(keywords);

			for(String kw: (Set<String>)keywords.keySet()) 
				if(!tokFreqs.containsKey(kw)) tokFreqs.put(kw, 0);

			for(String tok:(Set<String>) tokens.keySet()) 
				if(!keyFreqs.containsKey(tok)) keyFreqs.put(tok, 0);

			System.out.println(tokFreqs);
			System.out.println(keyFreqs);

			int count1, count2;
			int sum = 0, sum1 = 0, sum2 = 0;
			for(String tok: (Set<String>)tokFreqs.keySet()) {
				count1 = (Integer)tokFreqs.get(tok);	
				count2 = (Integer)keyFreqs.get(tok);	
				sum += count1*count2;
				sum1 += count1*count1;
				sum2 += count2*count2;	
			}
			
			score = sum/(Math.sqrt(sum1)*Math.sqrt(sum2));
			System.out.println(score);
			
		} catch (Exception e ) {}	

		return score;
	}

	private void closeDB() {
		try {
			conn.close(); // ** IMPORTANT : Close connections when done **
		} catch (Exception e) {}
	}

	public void setupDB() throws SQLException {
		    // Load the Oracle JDBC driver
		    DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());

		    // Connect to the database
		    // You must put a database name after the @ sign in the connection URL.
		    // You can use either the fully specified SQL*net syntax or a short cut
		    // syntax as <host>:<port>:<sid>.  The example uses the short cut syntax.
		    
		    //As index order => myusername, password, # of ads of Task 1~6
		    String arrayInput [] = new String [8];
		    
		    
		    // read system.in
		    try {
			    BufferedReader reader = new BufferedReader(new FileReader("system.in"));
			    String line;
			    int count = 0;
			    while((line = reader.readLine()) != null) {
			    	StringTokenizer tmp = new StringTokenizer(line);
			    	
			    	while(tmp.hasMoreElements()) {
			    		if(tmp.nextToken().equals("=")) {
			    			arrayInput[count] = tmp.nextToken();
			    			count++;
			    		}
			    	}
			    }
			    reader.close();
		    }	catch(IOException e) {
		    
		    }
		    
		    
		    String Queries = "create table Queries ( " +
		    		"qid INT PRIMARY KEY, query VARCHAR(400)" + ")";
		    String Advertisers = "create table Advertisers ( " +
		    		"advertiserId INT PRIMARY KEY, budget FLOAT, ctc FLOAT" +")";
		    String Keywords = "create table Keywords ( " +
		    		"advertiserId INT, keyword VARCHAR(100), bid FLOAT," + 
		    		"PRIMARY KEY (advertiserId, keyword)," + 
		    		"FOREIGN KEY(advertiserId)" + 
		    		"REFERENCES Advertisers(advertiserId)" + ")";
		    
			System.out.println("connecting DB ...");
		    //Connection conn =
			conn = 
		      DriverManager.getConnection ("jdbc:oracle:thin:hr/hr@oracle1.cise.ufl.edu:1521:orcl",
		                                   arrayInput[0], arrayInput[1]);
			System.out.println("DB connected.");
		    
		
		    // Create a Statement
		    Statement stmt = conn.createStatement ();

	/*	    
		try {    
		// drop tables
		stmt.executeUpdate("drop table Queries");
		//stmt.executeUpdate("drop table Keywords");
		//stmt.executeUpdate("drop table Advertisers");

		} catch (Exception e) {
		}
*/
		    // Create table
		 //   stmt.executeUpdate(Queries);
		 //   stmt.executeUpdate(Advertisers);
		//    stmt.executeUpdate(Keywords);
		    
		/*
		    //read queries.dat and insert data to table
		    try {
			    BufferedReader reader = new BufferedReader(new FileReader("Queries.dat"));
			    String line;
					pstmt = conn.prepareStatement("insert into Queries (qid, query) " + 
			    					"values(?,?)" ); 
			    while((line = reader.readLine()) != null) {
				System.out.println(line);
			    	StringTokenizer tmp = new StringTokenizer(line, "\t");
			    	
			    	while(tmp.hasMoreElements()) {
			    		pstmt.setInt(1, Integer.parseInt(tmp.nextToken()));
					pstmt.setString(2, tmp.nextToken());
					pstmt.executeUpdate();
			    		//stmt.executeUpdate("insert into Queries (qid, query) " + 
			    		//		"values(" + Integer.parseInt(tmp.nextToken()) + "," + "'" + tmp.nextToken() + "'" + ")");
			    		//System.out.print("insert into Queries (qid, query) " + 
			    		//		"values(" + Integer.parseInt(tmp.nextToken()) + "," + "'" + tmp.nextToken() + "'" + ")\n");
			    	}
			    }
			    reader.close();
		    }	catch(IOException e) {
		    
		    }
*/		    
		/*
		    // read advertisers.dat and insert data to table
		    try {
			    BufferedReader reader = new BufferedReader(new FileReader("Advertisers.dat"));
			    String line;
			    while((line = reader.readLine()) != null) {
			    	StringTokenizer tmp = new StringTokenizer(line);
			    	while(tmp.hasMoreElements()) {
//insert into queries(qid,query)(select to_number(regexp_substr(ss, ‘[^’ || chr(9) || ‘]+’,1,1)), regexp_substr(ss, ‘[^’ || chr(9) || ‘]+’,1,2) from q);
			    		stmt.executeUpdate("insert into Advertisers (advertiserId, budget, ctc) " + 
			    					"values(" + Integer.parseInt(tmp.nextToken()) + "," + 
			    					Float.parseFloat(tmp.nextToken()) + "," + Float.parseFloat(tmp.nextToken()) + ")");
			    	}
			    }
			    reader.close();
		    }	catch(IOException e) {
		    
		    }
		  */  
		    
		    // read keywords.dat and insert data to table
		/*
		    try {
			    BufferedReader reader = new BufferedReader(new FileReader("Keywords.dat"));
			    String line;
					pstmt = conn.prepareStatement("insert into Keywords (advertiserId, keyword, bid) " + 
			    					"values(?,?,?)" ); 
			    while((line = reader.readLine()) != null) {
				System.out.println(line);
			    	StringTokenizer tmp = new StringTokenizer(line);
			    	while(tmp.hasMoreElements()) {
			    		pstmt.setInt(1, Integer.parseInt(tmp.nextToken()));
					pstmt.setString(2, tmp.nextToken());
					pstmt.setFloat(3, Float.parseFloat(tmp.nextToken()));
					pstmt.executeUpdate();
					/*
			    		stmt.executeUpdate("insert into Keywords (advertiserId, keyword, bid) " + 
			    					"values(" + Integer.parseInt(tmp.nextToken()) + "," + 
			    					"'" + tmp.nextToken() + "'" + "," + Float.parseFloat(tmp.nextToken()) + ")");
					*/
	/*
			    	}
			    }
			    reader.close();
		    }	catch(IOException e) {
		    
		    }
		  */
  
		    /*
		    //write output
		    try {
		    	BufferedWriter bw = new BufferedWriter(new FileWriter("system.output.1"));
		    	String test = "123321";
		    	bw.write(test);
		    	bw.close();
		    }	catch(IOException e) {
		    	
		    }
*/

		    //conn.close(); // ** IMPORTANT : Close connections when done **
	
		  }
}

