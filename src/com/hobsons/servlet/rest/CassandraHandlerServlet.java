package com.hobsons.servlet.rest;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;

/*
 * 	   RESTful handler for working with C*
 *     
 *     Create = PUT
 *     Retrieve = GET
 *     Update = POST
 *     Delete = DELETE
 */

@SuppressWarnings("serial")
public class CassandraHandlerServlet extends HttpServlet {

	final static Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "10.24.0.210:9160");
	
	final static Keyspace keyspaceOperator = HFactory.createKeyspace("TestSpace", cluster);
	
	// Retrieve a record
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		
		long startTime = System.nanoTime();
		
		PrintWriter out = resp.getWriter();

		try {

			ColumnQuery<String, String, String> columnQuery = HFactory.createStringColumnQuery(keyspaceOperator);
			columnQuery.setColumnFamily("Standard1").setKey("jsmith").setName("first");
			QueryResult<HColumn<String, String>> result = columnQuery.execute();
			long elapsedTime = System.nanoTime() - startTime;
			
			System.out.println("DoGet Elapsed Time: " + elapsedTime + " ns");
			out.write(result.get().toString());

		} catch (HectorException e) {
			e.printStackTrace();
		}

		//cluster.getConnectionManager().shutdown();
		out.close();
	}

	@Override
	protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		resp.sendError(404);
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		resp.sendError(404);
	}

	@Override
	protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		resp.sendError(404);
	}
	

}
