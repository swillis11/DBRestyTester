package com.hobsons.servlet.rest;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


import static me.prettyprint.hector.api.factory.HFactory.createColumn;
import static me.prettyprint.hector.api.factory.HFactory.createKeyspace;
import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.getOrCreateCluster;

import me.prettyprint.cassandra.model.CqlQuery;
import me.prettyprint.cassandra.model.CqlRows;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.exceptions.HCassandraInternalException;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.query.QueryResult;



/*
 * RESTful handler for working with C*
 *
 * Create = PUT
 * Retrieve = GET
 * Update = POST
 * Delete = DELETE
 */

@SuppressWarnings("serial")
public class CQLHandlerServlet extends HttpServlet {

	
	  private final static String KEYSPACE = "Keyspace1";
	  private static final StringSerializer se = new StringSerializer();
	  private static final LongSerializer le = new LongSerializer();
	  private Keyspace keyspace;

	final static Cluster cluster = getOrCreateCluster("Test Cluster", "brisk1:9160,brisk2:9160,brisk3:9160");
	final static Keyspace keyspaceOperator = createKeyspace("TestSpace", cluster);
	

	// Retrieve a record
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		
		long startTime = System.nanoTime();
		
		try {
			
			CqlQuery<String,String,Long> cqlQuery = new CqlQuery<String,String,Long>(keyspace, se, se, le);
			cqlQuery.setQuery("select * from Standard1");
			QueryResult<CqlRows<String,String,Long>> result = cqlQuery.execute();

			System.out.println("QUERY RESULT:"+result.toString());
			long elapsedTime = System.nanoTime() - startTime;
			
			System.out.println("DoGet Elapsed Time: " + elapsedTime + " ns");



		} catch (HectorException e) {
			e.printStackTrace();
		}


	}

	/*
	 * 
	 * curl localhost:8088/cassandrahandlerservlet --get --request DELETE -d
	 * "cf=Standard1&rowkey=jsmith&id=dude1"
	 */
	@Override
	protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		long startTime = System.nanoTime();

		try{
		
			
    		
			long elapsedTime = System.nanoTime() - startTime;
			
			System.out.println("DoDelete Elapsed Time: " + elapsedTime + " ns");


        } catch (HectorException e) {
            e.printStackTrace();
        }  
	}

	/*
	 * curl localhost:8088/cassandrahandlerservlet --request POST -d
	 * "cf=Standard1&rowkey=jsmith&id=dude" Update = POST
	 */
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		long startTime = System.nanoTime();
		
        try {

    		

			long elapsedTime = System.nanoTime() - startTime;
			
			System.out.println("DoPost Elapsed Time: " + elapsedTime + " ns");

			
        } catch (HectorException e) {
            e.printStackTrace();
        }
}

	/*
	 * curl localhost:8088/cassandrahandlerservlet --request PUT -d "cf=Standard1&rowkey=123&id=dude&itworks=true"
	 * Create = PUT
	 * 
	 * Params: 
	 *  - RowKey = ?
	 *  - every other param will be a column/value in that row
	 */
	@Override
	protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		long startTime = System.nanoTime();
		
        try {

			long elapsedTime = System.nanoTime() - startTime;
			
			System.out.println("DoPut Elapsed Time: " + elapsedTime + " ns");
			
        } catch (Exception e) {
            e.printStackTrace();
        }

	}

}
