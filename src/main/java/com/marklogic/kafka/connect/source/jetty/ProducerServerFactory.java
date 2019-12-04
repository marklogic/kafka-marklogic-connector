package com.marklogic.kafka.connect.source.jetty;

import org.eclipse.jetty.server.*;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class ProducerServerFactory {

    private ProducerServerFactory() {}

    public static Server createServer(int port) {
        Server server = new Server(port);
        ServletHandler handler = new ServletHandler();
        server.setHandler(handler);
        // Main Servlet for publishing to Kafka
        ServletHolder holder = new ServletHolder(new ProducerServlet());
        handler.addServletWithMapping(holder, "/topics/*");
        // Redirect Servlet for root requests
        ServletHolder redirectHandler = new ServletHolder();
        redirectHandler.setServlet(new HttpServlet() {
            @Override
            protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
                resp.sendRedirect("/topics/");
            }
        });
        handler.addServletWithMapping(redirectHandler, "/*");
        return server;
    }

    public static Server createSecureServer(int port,
                                      String keystorePath,
                                      String keystorePassword,
                                      String keystoreManagerPassword,
                                      String truststorePath,
                                      String truststorePassword,
                                      boolean clientAuth) {
        Server server = new Server();

        // HTTP configuration
        HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.setSecureScheme("https");
        httpConfig.setSecurePort(8443);
        httpConfig.setSendServerVersion(true);
        httpConfig.setSendDateHeader(false);

        // Add the HTTP Connector
        //ServerConnector http = new ServerConnector(server, new HttpConnectionFactory(httpConfig));
        //http.setPort(port);
        //server.addConnector(http);

        // Configure SSL, KeyStore, TrustStore, Ciphers
        SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStorePath(keystorePath);
        sslContextFactory.setKeyStorePassword(keystorePassword);
        sslContextFactory.setKeyManagerPassword(keystoreManagerPassword);
        sslContextFactory.setTrustStorePath(truststorePath);
        sslContextFactory.setTrustStorePassword(truststorePassword);
        // force client authentication
        sslContextFactory.setNeedClientAuth(clientAuth);

        // SSL HTTP Configuration
        HttpConfiguration httpsConfig = new HttpConfiguration(httpConfig);
        httpsConfig.addCustomizer(new SecureRequestCustomizer());

        // Add the SSL HTTP Connector
        ServerConnector sslConnector = new ServerConnector(server,
                new SslConnectionFactory(sslContextFactory, "http/1.1"),
                new HttpConnectionFactory(httpsConfig));
        sslConnector.setPort(8443);
        server.addConnector(sslConnector);

        // Servlet Setup
        ServletHandler handler = new ServletHandler();
        server.setHandler(handler);
        // Main Servlet for publishing to Kafka
        ServletHolder holder = new ServletHolder(new ProducerServlet());
        handler.addServletWithMapping(holder, "/topics/*");
        // Redirect Servlet for root requests
        ServletHolder redirectHandler = new ServletHolder();
        redirectHandler.setServlet(new HttpServlet() {
            @Override
            protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
                resp.sendRedirect("/topics/");
            }
        });
        handler.addServletWithMapping(redirectHandler, "/*");

        return server;
    }
}
