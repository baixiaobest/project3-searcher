package edu.ucla.cs.cs144;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.util.Date;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;
import java.text.SimpleDateFormat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.* ;

import org.apache.lucene.document.Document;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import edu.ucla.cs.cs144.DbManager;
import edu.ucla.cs.cs144.SearchRegion;
import edu.ucla.cs.cs144.SearchResult;

public class AuctionSearch implements IAuctionSearch {

	/* 
         * You will probably have to use JDBC to access MySQL data
         * Lucene IndexSearcher class to lookup Lucene index.
         * Read the corresponding tutorial to learn about how to use these.
         *
	 * You may create helper functions or classes to simplify writing these
	 * methods. Make sure that your helper functions are not public,
         * so that they are not exposed to outside of this class.
         *
         * Any new classes that you create should be part of
         * edu.ucla.cs.cs144 package and their source files should be
         * placed at src/edu/ucla/cs/cs144.
         *
         */
    private static String INDEX_DIR = "/var/lib/lucene/index1";
    private IndexSearcher searcher = null;
    private QueryParser parser = null;
	
    
    
	public SearchResult[] basicSearch(String query, int numResultsToSkip, 
			int numResultsToReturn){
		// TODO: Your code here!
        try{
            if (numResultsToReturn <= 0)
                return new SearchResult[0];
            if (numResultsToSkip < 0)
                numResultsToSkip = 0;

        	searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(new File(INDEX_DIR))));
        	parser = new QueryParser("Content",new StandardAnalyzer());
        	Query myQuery = parser.parse(query);
        	TopDocs topDocs = searcher.search(myQuery, numResultsToSkip+numResultsToReturn);
        	ScoreDoc[] hits = topDocs.scoreDocs;
        
        	SearchResult[] results = new SearchResult[hits.length-numResultsToSkip];
        	int index=0;
        	for(int i=numResultsToSkip; i<hits.length; i++){
            	Document doc = searcher.doc(hits[i].doc);
            	results[index] = new SearchResult(doc.get("ID"), doc.get("Name"));
            	index++;
        	}
        
        	return results;
        }catch(IOException e){
            System.out.println("basicSearch IO Exception");
            return new SearchResult[0];
        }catch(ParseException e){
            System.out.println("basicSearch ParseException");
            return new SearchResult[0];
        }
	}

	public SearchResult[] spatialSearch(String query, SearchRegion region,
			int numResultsToSkip, int numResultsToReturn) {
		// TODO: Your code here!
        try{
            if (numResultsToReturn <= 0)
                return new SearchResult[0];
            if (numResultsToSkip < 0)
                numResultsToSkip = 0;

            SearchResult[] basicSearchResult = basicSearch(query, 0, 2147483647);

            /* load the driver*/
            Class.forName("com.mysql.jdbc.Driver"); 

            Connection c = null ;
            /* create an instance of a Connection object */
            c = DriverManager.getConnection("jdbc:mysql://localhost:3306/CS144", "cs144", ""); 

            Statement s = c.createStatement();

            ResultSet rs = s.executeQuery("SET @poly = GeomFromText('Polygon((" + region.getLx() + ' ' + region.getLy() + ','
                                                    + region.getRx() + ' ' + region.getLy() + ','
                                                    + region.getRx() + ' ' + region.getRy() + ','
                                                    + region.getLx() + ' ' + region.getRy() + ','
                                                    + region.getLx() + ' ' + region.getLy() + "))')");

            SearchResult[] ALLSearchResult = new SearchResult[basicSearchResult.length];

            int numOfAllResult = 0;
            for(int i=0; i<basicSearchResult.length; i++) {
                rs = s.executeQuery("SELECT ItemID FROM ItemGeo WHERE MBRContains(@poly, GeoPosition) AND ItemID = " + 
                                     basicSearchResult[i].getItemId());
    
                if (rs.next() && (numOfAllResult < numResultsToReturn+numResultsToSkip)) {
                    ALLSearchResult[numOfAllResult] = new SearchResult(basicSearchResult[i].getItemId(), basicSearchResult[i].getName());
                    numOfAllResult++;
                }
            }
            /* close the resultset, statement and connection */
            rs.close();
            s.close();
            c.close();

            SearchResult[] Result = new SearchResult[numOfAllResult - numResultsToSkip];
            int count = 0;
            for(int i=numResultsToSkip; i < numOfAllResult; i++, count++) {
                Result[count] = ALLSearchResult[i];
            }
            return Result;

        }catch (ClassNotFoundException ex){
            System.out.println(ex);
            return new SearchResult[0];
        }catch (SQLException ex){
            System.out.println("SQLException caught");
            System.out.println("---");
            while ( ex != null ){
                System.out.println("Message   : " + ex.getMessage());
                System.out.println("SQLState  : " + ex.getSQLState());
                System.out.println("ErrorCode : " + ex.getErrorCode());
                System.out.println("---");
                ex = ex.getNextException();
            }
            return new SearchResult[0];
        }
	}

	public String getXMLDataForItemId(String itemId) {
		// TODO: Your code here!
		return "";
	}
	
	public String echo(String message) {
		return message;
	}

}
