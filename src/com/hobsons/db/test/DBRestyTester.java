package com.hobsons.db.test;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import com.hobsons.servlet.rest.*;

public class DBRestyTester {

	public static void main(String[] args) throws Exception {
        Server server = new Server(8088);
        
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");
        server.setHandler(context);
        
        context.addServlet(new ServletHolder(new CassandraHandlerServlet()), "/cassandrahandlerservlet");
        context.addServlet(new ServletHolder(new MongoHandlerServlet()), "/mongohandlerservlet");
        
        System.out.println("REST server started");
        
        server.start();
        server.join();
        
	}
}
