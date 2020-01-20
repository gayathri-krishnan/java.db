package com.nielsen.session.java.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Hello world!
 *
 */
public class HelloJavaDb {
	Connection conn;

	public static void main(String[] args) throws SQLException {
		HelloJavaDb app = new HelloJavaDb();

		app.connectionToDerby();
		app.normalDbUsage();
		app.transactions();
		app.closeConnectionToDerby();
		
	}
	
	public void closeConnectionToDerby() throws SQLException {
		conn.close();
	}

	public void connectionToDerby() throws SQLException {
		// -------------------------------------------
		// URL format is
		// jdbc:derby:<local directory to save data>
		// -------------------------------------------
		String dbUrl = "jdbc:derby:c:\\Users\\krga9002\\MyDB\\demo;create=true";
		conn = DriverManager.getConnection(dbUrl);
		// further reading -
		// https://docs.oracle.com/javase/8/docs/api/java/sql/DriverManager.html
	}

	public void normalDbUsage() throws SQLException {
		String selectSql = "SELECT * FROM users";
		Statement stmt = conn.createStatement();
		// drop table
		// stmt.executeUpdate("Drop Table users");

		try {
			// create table
			stmt.executeUpdate("Create table users (id int primary key, name varchar(30))");

		} catch (Exception e) {
			// nothing to do. The table exists
		}

		// delete existing rows
		stmt.executeUpdate("delete from users");

		// insert 2 rows
		stmt.executeUpdate("insert into users values (1,'tom')");
		stmt.executeUpdate("insert into users values (2,'peter')");

		printValues(stmt,selectSql);

		// update
		stmt.executeUpdate("update users set name = 'harry' where name = 'tom'");

		//ResultSet
		printValues(stmt,selectSql);

		// ResultSet metadata
		getMetadata(stmt);

		// Prepared Statement
		PreparedStatement ps = conn.prepareStatement("insert into users values (?,?)");
		ps.setInt(1, 3);
		ps.setString(2, "larry");
		ps.execute();

		printValues(stmt,selectSql);
		
		//Close all
		//Excercise, try the same with try with resources.
		//HOw would you modify the total code to have a try with resources?
		ps.close();
		stmt.close();
		

	}

	public void printValues(Statement stmt, String sql) throws SQLException {
		// ResultSet
		ResultSet rs = stmt.executeQuery(sql);

		// print out query result
		while (rs.next()) {
			System.out.printf("%d\t%s\n", rs.getObject(1), rs.getObject(2));
		}
		System.out.println("============================");
	}

	public void transactions() throws SQLException {
		Statement stmt = conn.createStatement();
		try {
			// create table
			stmt.executeUpdate("Create table account (id int primary key, balance int)");

		} catch (Exception e) {
			// nothing to do. The table exists
		}
		stmt.executeUpdate("delete from account");
		stmt.executeUpdate("insert into account values (1, 100)");
		stmt.executeUpdate("insert into account values (2, 300)");
		
		printValues(stmt,"select * from account");
		
		//turn off default auto commit behaviour
		conn.setAutoCommit(false);
		//Transaction ACID proprties
		//https://en.wikipedia.org/wiki/ACID
		
		try {
			//credit
			credit(stmt, 100);
			//debit
			debit(stmt, 100);
			conn.commit();
			
		}catch(Exception e) {
			conn.rollback();
		}finally {
			conn.setAutoCommit(true);
		}
		printValues(stmt,"select * from account");
		
		stmt.close();
		
	}
	
	public void credit(Statement stmt , int amount) throws SQLException {
		stmt.executeUpdate("update account set balance = balance + " + amount + " where id = 1");
		//throw new SQLException(); --uncomment this line when you want to test rollback
	}
	
	public void debit(Statement stmt, int amount) throws SQLException {
		stmt.executeUpdate("update account set balance = balance - " + amount + " where id = 2");
	}
	
	public void getMetadata(Statement stmt) throws SQLException {
		// ResultSet
		ResultSet rs = stmt.executeQuery("SELECT * FROM users");

		ResultSetMetaData meta = rs.getMetaData();
 		for (int i = 1; i <= meta.getColumnCount(); i++) {
			System.out.println(meta.getColumnName(i));
			// What else can you see from meta?
		}
		System.out.println("============================");
	}
}
