package edu.hm.cs.bigdata.hana.persistence;

import java.sql.Date;

/**
 * 
 * @author Andreas Geiger
 * 
 * POJO class for illustrating the basic construct of the database table comparing 
 * to a Java program
 *
 */
public class BigData {
    private int id;
    private String userName;
    private Date createdAt;
    private String text;
    private int number;
    
	public int getId() {
		return id;
	}
	public void setId(int i) {
		this.id = i;
	}
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public Date getCreatedAt() {
		return createdAt;
	}
	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	public int getNumber() {
		return number;
	}
	public void setNumber(int number) {
		this.number = number;
	}

    
}
