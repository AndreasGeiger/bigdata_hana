package edu.hm.cs.bigdata.hana.servlet;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sap.security.core.server.csi.IXSSEncoder;
import com.sap.security.core.server.csi.XSSEncoder;

import edu.hm.cs.bigdata.hana.persistence.BigData;
import edu.hm.cs.bigdata.hana.persistence.BigDataDAO;

/**
 * 
 * @author Andreas Geiger
 * 
 * Servlet class for receiving and responding requests from the client to the web
 * server and vice versa.
 *
 */
public class BigDataServlet extends HttpServlet {
	private static final Logger LOGGER = LoggerFactory.getLogger(BigDataServlet.class);
    private static final long serialVersionUID = 1L;
    

    private BigDataDAO bigDataDAO;

    /**
     * Manage resources that are needed for the life of the servlet.
     * Set the connection between the SAP HANA database
     */
    @Override
    public void init() throws ServletException {
        try {
            InitialContext ctx = new InitialContext();
            DataSource ds = (DataSource) ctx.lookup("java:comp/env/jdbc/DefaultDB");
            bigDataDAO = new BigDataDAO(ds);
        } catch (SQLException e) {
            throw new ServletException(e);
        } catch (NamingException e) {
            throw new ServletException(e);
        }
    }
    
    /**
     * Handle HTTP POST requests
     * Generate a random data set and save it to the database
     * Refresh the user interface with the current data
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException,
            IOException {
        try {
        	generateData(1000);
            doGet(request, response);
        } catch (Exception e) {
            response.getWriter().println("Persistence operation failed with reason: " + e.getMessage());
            LOGGER.error("Persistence operation failed", e);
        }
    }

	/**
	 * Handle  HTTP GET requests
	 * Add the title, button and table as HTML to the user interface
	 */
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.getWriter().println("<h1 align='center'>Big Data with SAP HANA!</h1>");
        try {
        	addButtonToHTML(response);
            addDataToHTML(response);
            
        } catch (Exception e) {
            response.getWriter().println("Persistence operation failed with reason: " + e.getMessage());
            LOGGER.error("Persistence operation failed", e);
        }
    }
    
    /**
     * 
     * @param response
     * @throws SQLException
     * @throws IOException
     * 
     * Add as HTML the average length of the values in the column USER_NAME
     */
    private void addAnalyzedUserNameToHTML(HttpServletResponse response) throws SQLException, IOException {
    	String htmlToAdd = "<p align='center'>Average lenght of User_Name: "+ bigDataDAO.analyzeUsername() +"</p>";
    	response.getWriter().println(htmlToAdd);
    }
    
    /**
     * 
     * @param response
     * @throws SQLException
     * @throws IOException
     * 
     * Add as HTML the most frequently used date in the column CREATED_AT
     */
    private void addAnalyzedCreatedAtToHTML(HttpServletResponse response) throws SQLException, IOException {
    	String htmlToAdd = "<p align='center'>Date with highest frequency: " + bigDataDAO.analyzeCreatedAt() + "</p>";
    	response.getWriter().println(htmlToAdd);
    }

    /**
     * 
     * @param response
     * @throws SQLException
     * @throws IOException
     * 
     * Add as HTML the frequency of the letter 'e' used in the column TEXT
     */
    private void addAnalyzedTextToHTML(HttpServletResponse response) throws SQLException, IOException {
    	String htmlToAdd = "<p align='center'>Frequency of letter 'e': " + bigDataDAO.analyzeText() + "</p>";
    	response.getWriter().println(htmlToAdd);
    }
    
    /**
     * 
     * @param response
     * @throws SQLException
     * @throws IOException
     * 
     * Add as HTML the average of the numbers in column NUMBER
     */
    private void addAnalyzedNumberToHTML(HttpServletResponse response) throws SQLException, IOException {
    	String htmlToAdd = "<p align='center'>Average of numbers: " + bigDataDAO.analyzeNumber() + "</p>";
    	response.getWriter().println(htmlToAdd);
    }
    
    /**
     * 
     * @param response
     * @throws SQLException
     * @throws IOException
     * 
     * Add the analyzed data and the data in the table 'BIG_DATA' if possible
     */
    private void addDataToHTML(HttpServletResponse response) throws SQLException, IOException {
        List<BigData> dataset = bigDataDAO.selectAllData();
        
        response.getWriter().println(
                "<table width='100%' style='margin: auto; border-collapse:collapse;' border=\"1\"><tr><th colspan=\"5\">" + (dataset.isEmpty() ? "" : dataset.size() + " ")
                        + "Entries in the Database</th></tr>");
        if (dataset.isEmpty()) {
            response.getWriter().println("<tr><td colspan=\"5\">No Dataset found in the database</td></tr>");
        } else {
    		this.addAnalyzedUserNameToHTML(response);
    		this.addAnalyzedCreatedAtToHTML(response);
    		this.addAnalyzedTextToHTML(response);
    		this.addAnalyzedNumberToHTML(response);
            response.getWriter().println("<tr><th>ID</th><th>User name</th><th>Created at</th><th>Text</th><th>Number</th></tr>");
            IXSSEncoder xssEncoder = XSSEncoder.getInstance();
            for (BigData bg : dataset) {
                response.getWriter().println(
                        "<tr><td>" + xssEncoder.encodeHTML(Integer.toString(bg.getId())) + "</td><td>"
                                + xssEncoder.encodeHTML(bg.getUserName()) + "</td><td>" + bg.getCreatedAt() + "</td><td style='word-wrap: break-all'>" + bg.getText()+ "</td><td>" + bg.getNumber() + "</td></tr>");
            }
            response.getWriter().println("</table>");
        }
        
        
    }
    
    /**
     * 
     * @throws SQLException
     * 
     * Delete all data in the table 'BIG_DATA'
     */
    private void clearData() throws SQLException {
		bigDataDAO.clearTable();
		
	}
    
    /**
     * 
     * @param amountDataset
     * @throws SQLException
     * 
     * Either insert or update the create data set into the database
     */
    private void generateData(int amountDataset) throws SQLException {
    	int amountData = bigDataDAO.getAmountDataset();
    	if(amountData == 0) {
    		bigDataDAO.insertBigData(amountDataset);
    	} else {
    		bigDataDAO.updateBigData(amountDataset);
    	}
        
        
    }

    /**
     * 
     * @param response
     * @throws IOException
     * 
     * Add as HTML the form with the submit button
     */
    private void addButtonToHTML(HttpServletResponse response) throws IOException {
        response.getWriter().println(
                "<p align='center'><form align='center' action=\"\" method=\"post\">" + "<input type=\"submit\" value=\"Generate & Analyse Big Data\">" + "</form></p>");
    }
    
}

