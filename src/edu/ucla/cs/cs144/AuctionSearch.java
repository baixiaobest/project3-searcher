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
import java.sql.DriverManager;

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

//import java.text.ParseException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

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

            SearchResult[] basicSearchResult = basicSearch(query, 0, 10000000);

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
        try{
            //initialize mySQL
        	String returnXML="";
        	Class.forName("com.mysql.jdbc.Driver");
            Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/CS144", "cs144", "");
            
            //create statements
            Statement s = conn.createStatement();
            Statement categoryS = conn.createStatement();
            Statement buypriceS = conn.createStatement();
            Statement bidsS = conn.createStatement();
            Statement itemPosS = conn.createStatement();
        
            //Query data
        	ResultSet rs = s.executeQuery("Select * From ItemTable Where ItemID="+itemId);
            ResultSet category = categoryS.executeQuery("Select Category From ItemCategory Where ItemID="+itemId);
            ResultSet buyprice = buypriceS.executeQuery("Select Buy_Price From ItemBuyPrice Where ItemID="+itemId);
            ResultSet bids = bidsS.executeQuery("Select * From ItemBids Where ItemID="+itemId);
            ResultSet itemPos = itemPosS.executeQuery("Select Latitude, Longitude From ItemPosition Where ItemID="+itemId);
            
            //start filling up <Item> info
        	while(rs.next()){
        		returnXML += "<Item ItemID=\""+rs.getString("ItemID")+"\">\n";
            	returnXML += "  <Name>"+xmlEscape(rs.getString("Name"))+"</Name>\n";
                
                //get all categories
                while(category.next()){
                	returnXML +="  <Category>"+xmlEscape(category.getString("Category"))+"</Category>\n";
                }
                
                returnXML += "  <Currently>$"+rs.getString("Currently")+"</Currently>\n";
                if(buyprice.next()){
                    returnXML += "  <Buy_Price>$"+buyprice.getString("Buy_Price")+"</Buy_Price>\n";
                }
                returnXML += "  <First_Bid>$"+rs.getString("First_Bid")+"</First_Bid>\n";
                returnXML += "  <Number_of_Bids>"+rs.getString("Number_of_Bids")+"</Number_of_Bids>\n";
                
                //get all bids
                if(bids.next()){
                    returnXML += "  <Bids>\n";
                    do{
                        Statement bidderS = conn.createStatement();
                        Statement userLocationS = conn.createStatement();
                        ResultSet bidder = bidderS.executeQuery("Select Rating From BidderRating Where UserID='"+bids.getString("BidderID")+"'");
                        ResultSet userLocation = userLocationS.executeQuery("Select * from UserLocation Where UserID='"+bids.getString("BidderID")+"'");
                        
                        //start of a bid
                        bidder.next();
                        returnXML += "    <Bid>\n";
                        
                        //bidder
                        returnXML += "      <Bidder Rating=\""+bidder.getString("Rating")+"\" ";
                        returnXML += "UserID=\""+bids.getString("BidderID")+"\">\n";
                        //location and country
                        if(userLocation.next()){
                            String location = userLocation.getString("Location");
                            String country = userLocation.getString("Country");
                            if(location.length()>0)
                                returnXML += "        <Location>"+xmlEscape(location)+"</Location>\n";
                            if(country.length()>0)
                                returnXML += "        <Country>"+xmlEscape(country)+"</Country>\n";
                        }
                        returnXML += "      </Bidder>\n";
                        //time and amount
                        returnXML += "      <Time>"+reformatDate(bids.getString("Time"))+"</Time>\n";
                        returnXML += "      <Amount>$"+bids.getString("Amount")+"</Amount>\n";
                        returnXML += "    </Bid>\n";
                    }while(bids.next());
                    
                    returnXML += "  </Bids>\n";
                }else{
                    returnXML += "  <Bids />\n";
                }
                
                //item location
                returnXML += "  <Location";
                if(itemPos.next()){
                	returnXML += " Latitude=\""+itemPos.getString("Latitude")+"\" Longitude=\""+itemPos.getString("Longitude")+"\">";
                }else{
                	returnXML += ">";
                }
                returnXML += xmlEscape(rs.getString("Location"))+"</Location>\n";
                returnXML += "  <Country>"+xmlEscape(rs.getString("Country"))+"</Country>\n";
                returnXML += "  <Started>"+reformatDate(rs.getString("Started"))+"</Started>\n";
                returnXML += "  <Ends>"+reformatDate(rs.getString("Ends"))+"</Ends>\n";
                
                Statement sellerS = conn.createStatement();
                ResultSet seller = sellerS.executeQuery("Select Rating From SellerRating Where UserID='"+rs.getString("SellerID")+"'");
                seller.next();
                returnXML += "  <Seller Rating=\""+seller.getString("Rating")+"\" UserID=\""+rs.getString("SellerID")+"\" />\n";
                
                if(rs.getString("Description").length()>0)
                	returnXML += "  <Description>"+xmlEscape(rs.getString("Description"))+"</Description>\n";
                else
                    returnXML += "  <Description />\n";
                
                returnXML +="</Item>";
        	}
            
            
            try {
                conn.close();
            } catch (SQLException ex) {
                System.out.println(ex);
            }
        
			return returnXML;
        	}
        catch (SQLException ex){
            System.out.println("SQLException caught");
            System.out.println("---");
            while ( ex != null ){
                System.out.println("Message   : " + ex.getMessage());
                System.out.println("SQLState  : " + ex.getSQLState());
                System.out.println("ErrorCode : " + ex.getErrorCode());
                System.out.println("---");
                ex = ex.getNextException();
            }
            return "";
        }catch(ClassNotFoundException e){
            System.out.println("Class Not Found");
            return "";
        }
	}
    
    static String reformatDate(String dateString){
        try{
            SimpleDateFormat outputFormat = new SimpleDateFormat("MMM-dd-yy HH:mm:ss");
            SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date parsed = inputFormat.parse(dateString);
            return outputFormat.format(parsed);
        }catch(Exception e){
            System.out.println("Date parse failed");
            return "";
        }
    }
    
    static String xmlEscape(String str){
        return str.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;");
    }
	
	public String echo(String message) {
		return message;
	}

}
