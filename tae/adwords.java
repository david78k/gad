
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
	
	private void executeQuery(String query) {

	}

	private void search() {
		// compare a query with keywords from db
		// join Queries and Keywords tables	
		/*
			get keywords
			results = "select (tokenize query as keywords) from Queries, Keywords"	
		*/
		int qid = 77;
		String query = "select * from queries where qid = ?";
		query = "select * from keywords where keyword like '%?%'";
		query = "select * from keywords where ? like '%'|| keyword || '%'";
		String query2 = "http www.flickr.com photos 88145967 n00 24368586 in pool-32148876 n00";
//		query = "select * from keywords where '" + query2 + "' like '%'|| keyword || '%'";
		System.out.println(query);
		System.out.println(query2);

	    	StringTokenizer tmp = new StringTokenizer(query);
		Vector<String> tokens = new Vector<String>();
		String tok;	
	    	while(tmp.hasMoreElements()) {
			tok = tmp.nextToken();
			//System.out.print (tok + ", " );
			tokens.add(tok);
		}
		System.out.println(tokens);

		int n = 0;

		try {
			pstmt = conn.prepareStatement(query);
			pstmt.setString(1, query2);
				
			ResultSet rs = pstmt.executeQuery();
			System.out.println("query executed");
			System.out.println(rs);
 
			List ranks = new ArrayList();
			int aid;
			String keyword;
			double bid, ctc;
			double score, adrank;

				System.out.println( "advertiserid keyword bid");
			while (rs.next()) {
 
				aid = rs.getInt("advertiserid");
				keyword = rs.getString("keyword");
				bid = rs.getDouble("bid");
				
				System.out.println((++n) + " " + aid  + " " + keyword + " " + bid );
				//System.out.println((++n) + "advertiserid : " + aid  + " " + keyword + );
				//System.out.println("keyword : " + keyword);
				//System.out.println("bid : " + bid);
 
				// Keyword k = new Keyword(aid, keyword, bid);
				ctc = getCTC(aid);
				score = similarity(tokens, aid, keyword);
				//score = similarity(query2, aid, keyword);
				adrank = bid * ctc * score;
				// ranks.add(k, adrank);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	private void rank() {
		// AdRank = bid*ctc*similarity
		// select keywords.bid*advertisers.ctc*(select count(*) from queries) as adrank from keywords,advertisers where keywords.keyword = 'photos' and keywords.advertiserid = 878 and advertisers.advertiserid = keywords.advertiserid order by adrank;
		// select keywords.bid*advertisers.ctc as adrank from keywords,advertisers where keywords.keyword = 'photos' and keywords.advertiserid = 878 and advertisers.advertiserid = keywords.advertiserid order by adrank;
	}

	private void charge() {
		// only for first 100*x% ctc impressions
		// repeat every 100 impressions
	}
		
	private double getCTC(int advertiserid) {
		return 1;
	}
	
	//private double similarity(String query, Keyword keyword) {
	//private double similarity(String query, int aid, String keyword) {
	private double similarity(Vector<String> tokens, int aid, String keyword) {
		double score = 1;
		//System.out.println(query +" " + aid + " " + keyword);
		System.out.println(aid + " " + keyword);

		//String query3 = "select * from keywords where keyword = ?";
		//pstmt.setString(1, keyword);

		String query2 = "select * from keywords where keywords.advertiserid = ?";

		try {
			//List keywords = new ArrayList();
			Vector keywords = new Vector();
			pstmt = conn.prepareStatement(query2);	
			pstmt.setInt(1, aid);
			ResultSet rs = pstmt.executeQuery();
			while(rs.next()) {
				System.out.println(rs.getInt("advertiserid") + " " + rs.getString("keyword"));
				// keywords.add(k);	
			}

			
		} catch (Exception e ) {}	

		return score;
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
			aw.rank();
			aw.charge();

			aw.closeDB();
		}

	private void closeDB() {
		try {
			conn.close(); // ** IMPORTANT : Close connections when done **
		} catch (Exception e) {}
	}

	//public void setupDB() {
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
			    		// 제가 말씀드린 부분이 여기에요.. print는 제대로 되는데 oracle에 넣을때는 말씀드린 그 에러가 나는거 같아요. 35번째 test케이스에서 걸려요.
			    		// 오라클에서 돌리면..
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

