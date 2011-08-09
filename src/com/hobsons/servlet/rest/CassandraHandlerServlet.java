package com.hobsons.servlet.rest;

import java.io.IOException;
import java.io.PrintWriter;
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

		out.close();
	}

	
	/*
	 * 
	 *curl localhost:8088/cassandrahandlerservlet --get --request DELETE -d "cf=Standard1&rowkey=jsmith&id=dude1" 
	 *  
	 */
	@Override
	protected void doDelete(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		long startTime = System.nanoTime();
		
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, StringSerializer.get());            
    		Map<?,?> paramMap = req.getParameterMap();    		
    		String rowkey = ((String[])paramMap.get("rowkey"))[0];
    		String cf = ((String[])paramMap.get("cf"))[0];
    		
    		for (Map.Entry<?, ?> entry : paramMap.entrySet()){
    			
    			Object key  = entry.getKey();  
    			
       			if(!("rowkey".equals((String)(key))||"cf".equals((String)key))){
    				
    				mutator.addDeletion(rowkey, cf, (String)key, StringSerializer.get());
    			}
    			
    		}mutator.execute();
    		
			long elapsedTime = System.nanoTime() - startTime;
			
			System.out.println("DoDelete Elapsed Time: " + elapsedTime + " ns");

        } catch (HectorException e) {
            e.printStackTrace();
        }  
	}

	/*
	 * curl localhost:8088/cassandrahandlerservlet --request POST -d "cf=Standard1&rowkey=jsmith&id=dude"
	 * Update = POST
	 * 
	 */
	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

		long startTime = System.nanoTime();
		
        try {
            Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, StringSerializer.get());
            
    		Map<?,?> paramMap = req.getParameterMap();
    		
    		String rowkey = ((String[])paramMap.get("rowkey"))[0];
    		String cf = ((String[])paramMap.get("cf"))[0];
    		
    		for (Map.Entry<?, ?> entry : paramMap.entrySet()){
    			
    			Object key  = entry.getKey();
    			String[] params = (String[])paramMap.get(key);
 
    			if(!("rowkey".equals((String)(key))||"cf".equals((String)key))){
    					
    				mutator.addInsertion(rowkey, cf, HFactory.createStringColumn((String)key, params[0]));
    			}
    		}
    		
    		mutator.execute();

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
            Mutator<String> mutator = HFactory.createMutator(keyspaceOperator, StringSerializer.get());
            
    		Map<?,?> paramMap = req.getParameterMap();
    		
    		String rowkey = ((String[])paramMap.get("rowkey"))[0];
    		String cf = ((String[])paramMap.get("cf"))[0];
    		
    		for (Map.Entry<?, ?> entry : paramMap.entrySet()){
    			
    			Object key  = entry.getKey();
    			String[] params = (String[])paramMap.get(key);
 
    			if(!("rowkey".equals((String)(key))||"cf".equals((String)key))){
    					
    				mutator.addInsertion(rowkey, cf, HFactory.createStringColumn((String)key, params[0]));
    			}
    		}
    		
    		mutator.execute();

			long elapsedTime = System.nanoTime() - startTime;
			
			System.out.println("DoPut Elapsed Time: " + elapsedTime + " ns");
			
        } catch (HectorException e) {
            e.printStackTrace();
        }

	}

}
