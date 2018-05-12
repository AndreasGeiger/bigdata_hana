package edu.hm.cs.bigdata.hana.persistence;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import edu.hm.cs.bigdata.hana.util.Util;

/**
 * 
 * @author Andreas Geiger
 * 
 * Data Access Object to connect between the servlet and the SAP HANA database
 *
 */
public class BigDataDAO {
    private DataSource dataSource;
    
    /**
     * 
     * @param sqlQuery
     * @param isInsert
     * @param amountDataset
     * @throws SQLException
     * 
     * Method for dynamically execute a SQL Query to the SAP HANA database. You can vary between INSERT and UPDATE.
     * This methods creates a random data set which is either inserted or updated in the database 
     */
    private void executeInsertOrUpdate(String sqlQuery, boolean isInsert, int amountDataset) throws SQLException {
    	 Connection connection = dataSource.getConnection();
    	 Util utility = new Util();
    	 //Create random data set
         ArrayList<BigData> dataset = utility.createData(amountDataset);
         try {
        	if(isInsert) { //insert
             	for(BigData bg : dataset) {
           		  PreparedStatement pstmt = connection
                             .prepareStatement(sqlQuery);
           		  	 pstmt.setInt(1, bg.getId());
                     pstmt.setString(2, bg.getUserName());
                     pstmt.setDate(3, bg.getCreatedAt());
                     pstmt.setString(4, bg.getText());
                     pstmt.setInt(5, bg.getNumber());
                     pstmt.executeUpdate();
                     pstmt.close();
             	}
        	} else { //update
             	for(BigData bg : dataset) {
             		  PreparedStatement pstmt = connection
                               .prepareStatement(sqlQuery + bg.getId());
                       pstmt.setString(1, bg.getUserName());
                       pstmt.setDate(2, bg.getCreatedAt());
                       pstmt.setString(3, bg.getText());
                       pstmt.setInt(4, bg.getNumber());
                       pstmt.executeUpdate();
                       pstmt.close();
               	}
           	}
           
         } finally {
             if (connection != null) {
                 connection.close();
             }
         }
    }
    
    /**
     * 
     * @param sqlQuery
     * @return a number in the first row of the returned result
     * @throws SQLException
     * 
     * Executes dynamically the SQL Query to the database
     */
    private int executeSQLQueryInt(String sqlQuery) throws SQLException {
    	int rowCount;
    	Connection connection = dataSource.getConnection();
    	
    	try {
    		PreparedStatement pstmt = connection
        			.prepareStatement(sqlQuery);
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            rowCount = rs.getInt(1);
            pstmt.close();
            rs.close();
    	} finally {
    		if (connection != null) {
    			connection.close();
    		}
    	}
    	
    	return rowCount;
    }
    
    /**
     * 
     * @param sqlQuery
     * @return a date in the first row of the returned result
     * @throws SQLException
     * 
     * Executes dynamically the SQL Query to the database
     */
    private Date executeSQLQueryDate(String sqlQuery) throws SQLException {
    	Date rowCount;
    	Connection connection = dataSource.getConnection();
    	
    	try {
    		PreparedStatement pstmt = connection
        			.prepareStatement(sqlQuery);
            ResultSet rs = pstmt.executeQuery();
            rs.next();
            rowCount = rs.getDate(1);
            pstmt.close();
            rs.close();
    	} finally {
    		if (connection != null) {
    			connection.close();
    		}
    	}
    	
    	return rowCount;
    }

    /**
     * 
     * @param sqlQuery
     * @return a list of BigData objects according to the executed SQL statement
     * @throws SQLException
     * 
     * Executes dynamically the SQL Query to the database
     */
    private List<BigData> executeSqLQueryList(String sqlQuery) throws SQLException {
    	Connection connection = dataSource.getConnection();
        try {
        	PreparedStatement pstmt = connection
        			.prepareStatement(sqlQuery);
            ResultSet rs = pstmt.executeQuery();
            ArrayList<BigData> list = new ArrayList<BigData>();
            while (rs.next()) {
            	BigData bg = new BigData();
                bg.setId(rs.getInt(1));
                bg.setUserName(rs.getString(2));
                bg.setCreatedAt(rs.getDate(3));
                bg.setText(rs.getString(4));
                bg.setNumber(rs.getInt(5));
                list.add(bg);
            }
            pstmt.close();
            rs.close();
            return list;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
    
    /**
     * 
     * @throws SQLException
     * 
     * Checks if the database table 'BIG_DATA' exists. If not it
     * will be created.
     */
    private void checkTable() throws SQLException {
        Connection connection = null;

        try {
            connection = dataSource.getConnection();
            if (!tableExists(connection)) {
                createTable(connection);
            }
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
    
    /**
     * 
     * @param connection
     * @return true if the database table 'BIG_DATA' exists.
     * @throws SQLException
     */
    private boolean tableExists(Connection connection) throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        ResultSet rs = meta.getTables(null, null, "BIG_DATA", null);
        while (rs.next()) {
            String name = rs.getString("TABLE_NAME");
            if (name.equals("BIG_DATA")) {
                return true;
            }
        }
        rs.close();
        return false;
    }


    /**
     * 
     * @param connection
     * @throws SQLException
     * 
     * Create the database table 'BIG_DATA'
     */
    private void createTable(Connection connection) throws SQLException {
    	PreparedStatement pstmt = connection
                .prepareStatement("CREATE TABLE BIG_DATA "
    			+ "(ID INTEGER PRIMARY KEY NOT NULL, "
    			+ "USER_NAME VARCHAR (50),"
    			+ "CREATED_AT DATE, "
    			+ "TEXT VARCHAR (280), "
    			+ "NUMBER INTEGER)");
    	pstmt.executeUpdate();
    }
    
    /**
     * 
     * @param newDataSource
     * @throws SQLException
     * 
     * Constructor which sets the data source and checks if the table
     * 'BIG_DATA' exists.
     */
    public BigDataDAO(DataSource newDataSource) throws SQLException {
        setDataSource(newDataSource);
    }


    /**
     * 
     * @return the current data source
     */
    public DataSource getDataSource() {
        return dataSource;
    }


    /**
     * 
     * @param newDataSource
     * @throws SQLException
     * 
     * Set the current data source and checks if the table 'BIG_DATA' exists.
     */
    public void setDataSource(DataSource newDataSource) throws SQLException {
        this.dataSource = newDataSource;
        checkTable();
    }
    
    /**
     * 
     * @param amountDataset
     * @throws SQLException
     * 
     * Insert the amount of data specified.
     */
    public void insertBigData(int amountDataset) throws SQLException {
    	
    	this.executeInsertOrUpdate("INSERT INTO BIG_DATA (ID, USER_NAME, CREATED_AT, TEXT, NUMBER) VALUES (?, ?, ?, ?, ?)", true, amountDataset);
    }
    
    /**
     * 
     * @param amountDataset
     * @throws SQLException
     * 
     * Update the amount of data specified.
     */
    public void updateBigData(int amountDataset) throws SQLException {
    	this.executeInsertOrUpdate("UPDATE BIG_DATA SET USER_NAME = ?, CREATED_AT = ?, TEXT = ?, NUMBER = ? WHERE ID = ", false, amountDataset);
    }

    /**
     * Delete all data in the table 'BIG_DATA'
     */
    public void clearTable() throws SQLException {
    	String sqlQuery = "DELETE FROM BIG_DATA";
    	Connection connection = dataSource.getConnection();
    	try {
    	 PreparedStatement pstmt = connection
                 .prepareStatement(sqlQuery);
         pstmt.executeUpdate();
         pstmt.close();
    	} finally {
            if (connection != null) {
            	connection.close();
            }
    	}
    }

    /**
     * 
     * @return all data in the table 'BIG_DATA'
     * @throws SQLException
     */
    public List<BigData> selectAllData() throws SQLException {
    	String sqlQuery = "Select * FROM BIG_DATA";
    	return this.executeSqLQueryList(sqlQuery);
    }
    
    /**
     * 
     * @return the amount of dataset in the table 'BIG_DATA'
     * @throws SQLException
     */
    public int getAmountDataset() throws SQLException {
    	String sqlQuery = "SELECT COUNT(*) FROM BIG_DATA";
    	return this.executeSQLQueryInt(sqlQuery);
    }
 
    /**
     * 
     * @return the average length of the values in the column USER_NAME 
     * 			of the table 'BIG_DATA'
     * @throws SQLException
     */
    public int analyzeUsername() throws SQLException {
    	String sqlQuery = "SELECT AVG(LENGTH(USER_NAME)) FROM BIG_DATA";
    	return this.executeSQLQueryInt(sqlQuery);
    	
    }
    
    /**
     * 
     * @return the most common date in the column CREATED_AT in the 
     * 			table 'BIG_DATA'
     * @throws SQLException
     */
    public Date analyzeCreatedAt() throws SQLException {
    	String sqlQuery = "SELECT CREATED_AT FROM BIG_DATA GROUP BY CREATED_AT ORDER BY COUNT(*) DESC LIMIT 1;";
    	return this.executeSQLQueryDate(sqlQuery);
    }
    
    /**
     * 
     * @return the number of letter 'e' used in the column
     * 			TEXT of the table 'BIG_DATA'
     * @throws SQLException
     */
    public int analyzeText() throws SQLException {
    	String sqlQuery = "SELECT SUM(LENGTH(TEXT) - LENGTH(REPLACE(TEXT, 'e', ''))) FROM BIG_DATA";
    	return this.executeSQLQueryInt(sqlQuery);
    }
    
    /**
     * 
     * @return the average of the values in column NUMBER of the 
     * 			table 'BIG_DATA'
     * @throws SQLException
     */
    public int analyzeNumber() throws SQLException {
    	String sqlQuery = "SELECT AVG(TO_DOUBLE(NUMBER)) FROM BIG_DATA";
    	return this.executeSQLQueryInt(sqlQuery);
    }
}
