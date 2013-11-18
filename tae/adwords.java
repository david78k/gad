
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
public class adwords {

	/**
	 * @param args
	 */
	  public static void main (String args [])
		       throws SQLException
		  {
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
		    

	    

		    Connection conn =
		      DriverManager.getConnection ("jdbc:oracle:thin:hr/hr@oracle1.cise.ufl.edu:1521:orcl",
		                                   arrayInput[0], arrayInput[1]);
		    
		    // Create a Statement
		    Statement stmt = conn.createStatement ();

        	PreparedStatement pstmt = null;   
      
	/*	    
		try {    
		// drop tables
		stmt.executeUpdate("drop table Queries");
		//stmt.executeUpdate("drop table Keywords");
		//stmt.executeUpdate("drop table Advertisers");

		} catch (Exception e) {
		}

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
		    
		/*
		    // read advertisers.dat and insert data to table
		    try {
			    BufferedReader reader = new BufferedReader(new FileReader("Advertisers.dat"));
			    String line;
			    while((line = reader.readLine()) != null) {
			    	StringTokenizer tmp = new StringTokenizer(line);
			    	while(tmp.hasMoreElements()) {
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

		    conn.close(); // ** IMPORTANT : Close connections when done **
	
		  }
}

