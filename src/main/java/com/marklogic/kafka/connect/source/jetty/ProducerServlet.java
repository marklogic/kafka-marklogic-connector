package com.marklogic.kafka.connect.source.jetty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.marklogic.kafka.connect.source.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.stream.Collectors;

public class ProducerServlet extends HttpServlet {

    private static Logger logger = LoggerFactory.getLogger(ProducerServlet.class);

    private MessageQueue queue = MessageQueue.getInstance();

    protected String getTopicFromPath(String path) {
        path = path.replaceFirst("/", "");
        path = path.replaceAll("/", ".");
        return path;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        logger.info("in ProducerServlet doPost()");

        String mimeType = req.getHeader("Content-Type");
        String topic = getTopicFromPath(req.getPathInfo());
        String content = req.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

        MessageQueue.Message msg = new MessageQueue.Message(topic, mimeType, content);
        queue.enqueue(msg);

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType("application/json");
        ObjectMapper mapper = new ObjectMapper();
        resp.getWriter().println(mapper.writeValueAsString(msg));
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        logger.info("in ProducerServlet doGet()");
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType("text/html");

        StringBuilder page = new StringBuilder();

        page.append("<html>");
        page.append("<head>");
        page.append("<title>MarkLogic Kafka Connector - Producer service</title>");
        page.append("</head>");
        page.append("<body>");
        page.append("<div>");
        page.append("<h1>USAGE:</h1>");
        page.append("<span>Post {url}/topics/{topic}<br>Body: {payload}</span>");
        page.append("</div>");
        page.append("</body>");
        page.append("</html>");

        resp.getWriter().println(page.toString());
    }
}
