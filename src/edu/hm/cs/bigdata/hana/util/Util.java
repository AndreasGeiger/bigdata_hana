package edu.hm.cs.bigdata.hana.util;

import java.sql.Date;
import java.util.ArrayList;

import edu.hm.cs.bigdata.hana.persistence.BigData;

/**
 * 
 * @author Andreas Geiger
 * 
 * Utility class for creating random values
 *
 */
public class Util {
	
	private int id = 0;
	
	/**
	 * 
	 * @return a random number between 0 and Integer.MAX_VALUE (2^31 - 1)
	 */
	private int createRandomInt() {
		return (int) (Math.random() *Integer.MAX_VALUE);
	}
	
	/**
	 * 
	 * @param maxLengthWord
	 * @return a random String which can contain upper, lower letters and digits
	 */
	private String createRandomText(int maxLengthWord) {
			
		String charactersAndDigits = "ABCDEFGHIJKLMNOPQRSTUVWXYZ abcdefghijklmnopqrstuvwxyz 1234567890";
        StringBuilder randomString = new StringBuilder();
        int randomLenghtWord = (int) (Math.random() * maxLengthWord);
        while (randomString.length() < randomLenghtWord) {
            int index = (int) (Math.random() * charactersAndDigits.length());
            randomString.append(charactersAndDigits.charAt(index));
        }
        String saltStr = randomString.toString();
        return saltStr;
		}
	
	/**
	 * 
	 * @return a random date
	 */
	@SuppressWarnings("deprecation") //need to use java.sql.Date because of PreparedStatement
	private Date createRandomDate() {
		int randomYear = (int) (Math.random() * 4000) + 1000; //Range: 1000 - 5000
		int randomMonth = (int) (Math.random() * 12) + 1; //Range: 1 - 12
		
		int randomDay = (int) (Math.random() * 28) + 1; // Range: 1 - 28
		
		return new Date(randomYear, randomMonth, randomDay);
	}
	
	/**
	 * 
	 * @param amountDataset
	 * @return the list of created objects
	 * 
	 * By default is the maximum length of the created user name 50
	 * By default is the maximum length of the created text 280
	 */
	public ArrayList<BigData> createData(int amountDataset) {
		
		ArrayList<BigData> dataset = new ArrayList<BigData>();
		int maxLengthUserName = 50;
		int maxLengthText = 280;
		
		for(int i = 0; i<amountDataset; i++) {
			BigData bigData = new BigData();
			id++;
			bigData.setId(id);
			bigData.setUserName(this.createRandomText(maxLengthUserName));
			bigData.setCreatedAt(this.createRandomDate());
			bigData.setText(this.createRandomText(maxLengthText));
			bigData.setNumber(this.createRandomInt());
			dataset.add(bigData);
		}
		
		return dataset;
	}

}
