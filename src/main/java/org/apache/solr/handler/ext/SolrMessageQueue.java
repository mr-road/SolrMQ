package org.apache.solr.handler.ext;

import com.rabbitmq.client.*;
import java.io.IOException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;

public class SolrMessageQueue extends RequestHandlerBase implements SolrCoreAware {

    protected String mqHost;
    protected ConnectionFactory factory;
    protected String queue;
    //protected Integer queueDequeueDelay;
    protected String plugin_handler;
    protected Boolean durable = Boolean.TRUE;
    protected SolrCore core;

    Logger  logger = Logger.getLogger("org.apache.solr.handler.ext.SolrMessageQueue");
    
    public SolrMessageQueue() {}

    @Override
    public void init(NamedList args) {
        super.init(args);
        //this.initArgs.get("messageQueueHost");
        mqHost = (String) this.initArgs.get("messageQueueHost");
        queue = (String) this.initArgs.get("queue");
        plugin_handler = (String) this.initArgs.get("updateHandlerName");
        factory = new ConnectionFactory();
        factory.setHost(mqHost);

        QueueListener listener = new QueueListener();
        listener.start();
    }

    @Override
    public String getDescription() {
        return "SOLR MessageQueue listener";
    }

    @Override
    public String getSource() {
        return "$Source$";
    }

    @Override
    public String getSourceId() {
        return "$Id$";
    }

    @Override
    public String getVersion() {
        return "$Revision$";
    }

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws IOException  {
        rsp.add("description", "This is a simple message queueing plugin for solr.");
        rsp.add("host", mqHost);
        rsp.add("queue", queue);
        rsp.add("handler", plugin_handler);
        rsp.add("durable", durable.toString());
    }
	
    

    /**
    * This gives us a handle to the SolrCore
    *  @param core - the SolrCore
    */
    public void inform(SolrCore core) {
            this.core = core;
    }

    /**
    * Listener thread. This is the core listener.
    * Any message consumed spawns a new thread for handling. 
    *
    * @author rnoble
    * @author jatherton
    */
    private class QueueListener extends Thread{

        private boolean exclusive = false;
        private boolean autoDelete = false;
        private java.util.Map<String, Object> nullArgs = null;
        private boolean autoAck = true;

        @Override
        public void run() {
            Connection connection;

            try {
                logger.log(Level.INFO, "Started QueueListener");
                connection = factory.newConnection();
                Channel channel = connection.createChannel();
                channel.queueDeclare(queue, durable.booleanValue(), exclusive, autoDelete, nullArgs);
                QueueingConsumer consumer = new QueueingConsumer(channel);
                channel.basicConsume(queue, autoAck, consumer);
                logger.log(Level.INFO, "Configured QueueListener");
                while (true) {
                    QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                    QueueUpdateWorker worker = new QueueUpdateWorker(delivery, plugin_handler);
                    worker.start();
                }
            } catch (IOException e) {
                 logger.log(Level.ERROR, e.getMessage());
            } catch (ShutdownSignalException e) {
                 logger.log(Level.ERROR, e.getMessage());
            } catch (ConsumerCancelledException e) {
                logger.log(Level.ERROR, e.getMessage());
            } catch (InterruptedException e) {
                logger.log(Level.ERROR, e.getMessage());
            }
            logger.log(Level.INFO, "Exiting QueueListener");
        }
    }
}
