package com.hobsons.servlet.rest;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

	/*
	 * curl localhost:8088/cassandrahandlerservlet --request POST -d "id=dude"
	 * Update = POST
	 * 
	 */
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		String id = req.getParameter("id");
		System.out.println("Param = " + id);
		resp.setStatus(200);
	}

	/*
	 * curl localhost:8088/cassandrahandlerservlet --request PUT -d "rowkey=123&id=dude&itworks=true"
	 * Create = PUT
	 * 
	 * Params: 
	 *  - RowKey = ?
	 *  - every other param will be a column/value in that row
	 */
	@Override
	protected void doPut(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		//TODO Add handler for inserting data into the Test Cluster. Same keyspace. Create a Row Key and a Column.
		
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, StringSerializer.get());
            
    		Map<?,?> paramMap = req.getParameterMap();
    		
    		String rowkey = ((String[])paramMap.get("rowkey"))[0];
    		
    		for (Map.Entry<?, ?> entry : paramMap.entrySet()){
    			
    			Object key  = entry.getKey();
    			String[] params = (String[])paramMap.get(key);
    			
    			mutator.insert(rowkey, "Standard1", HFactory.createStringColumn((String)key, params[0]));

    		    System.out.println(key + "=" + params[0]);
    		}         

        } catch (HectorException e) {
            e.printStackTrace();
        }
        cluster.getConnectionManager().shutdown();
        
        
/*		
		Map<?,?> paramMap = req.getParameterMap();

		for (Map.Entry<?, ?> entry : paramMap.entrySet())
		{
			Object key  = entry.getKey();
			String[] params = (String[])paramMap.get(key);
			
		    System.out.println(key + "=" + params[0]);
		}
		
		System.out.println("RowKey = " + ((String[])paramMap.get("rowkey"))[0]);
		
		resp.setStatus(200);
*/
	}

}
