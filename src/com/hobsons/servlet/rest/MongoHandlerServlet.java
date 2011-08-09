package com.hobsons.servlet.rest;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import me.prettyprint.hector.api.exceptions.HectorException;

import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;


@SuppressWarnings("serial")
public class MongoHandlerServlet extends HttpServlet{

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {

		
		try{
			Mongo m = new Mongo("graylog2",27017);
			DB db = m.getDB( "mydb" );
			
			Set<String> colls = db.getCollectionNames();
	
			for (String s : colls) {
			    System.out.println(s);
			}
		}catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		Mongo m = new Mongo("graylog2",27017);
		DB db = m.getDB( "mydb" );
	}

	@Override
	protected void doPut(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		Mongo m = new Mongo("graylog2",27017);
		DB db = m.getDB( "mydb" );
	}

	@Override
	protected void doDelete(HttpServletRequest req, HttpServletResponse resp)
			throws ServletException, IOException {
		Mongo m = new Mongo("graylog2",27017);
		DB db = m.getDB( "mydb" );
	}

}
