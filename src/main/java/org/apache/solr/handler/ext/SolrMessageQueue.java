package org.apache.solr.handler.ext;

import com.rabbitmq.client.*;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.solr.common.params.MultiMapSolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.util.plugin.SolrCoreAware;

public class SolrMessageQueue extends RequestHandlerBase implements SolrCoreAware {

    protected String mqHost;
    protected ConnectionFactory factory;
    protected String queue;
    protected String plugin_handler;
    protected Boolean durable = Boolean.TRUE;
    protected SolrCore core;

    Logger  logger = Logger.getLogger("org.apache.solr.handler.ext.SolrMessageQueue");
    
    public SolrMessageQueue() {}

    @Override
    public void init(NamedList args) {
        super.init(args);
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
    * Performs the actual update request
    * @param handler - name of the handler, like /update or /update/json. Should probably be loaded.
    * @param params - the parameters, these can be parsed as custom message headers
    * @param message - the actual message, at present only strings are allowed.
    * @return SolrQueryResponse - returns the actual response. Check the Exception to handle faults
    */
    public SolrQueryResponse performUpdateRequest(String handler, Map<String, String[]> params, String message){

        MultiMapSolrParams solrParams = new MultiMapSolrParams(params);
        SolrRequestHandler requestHandler = core.getRequestHandler(handler);

        ContentStream stream = new ContentStreamBase.StringStream(message);
        ArrayList<ContentStream> streams = new ArrayList<ContentStream>();
        streams.add(stream);
        
        SolrQueryRequestBase request = new SolrQueryRequestBase(core, solrParams){};
        request.setContentStreams(streams);
        SolrQueryResponse response = new SolrQueryResponse();

        core.execute(requestHandler, request, response);
        return response;
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
    *
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
                    QueueUpdateWorker worker = new QueueUpdateWorker(delivery);
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

    /**
    * Worker thread. This is spawned for each message consumed.
    * @author rnoble
    * @author jatherton
    */
    public  class QueueUpdateWorker extends Thread{ 
        QueueingConsumer.Delivery delivery;
        
        public QueueUpdateWorker(QueueingConsumer.Delivery delivery){
            super();
            this.delivery = delivery;
        }

        @Override
        public void run(){
            String message =  "";
            try
            {
                message = new MessageDecoder().Decode(delivery.getBody());
            }
            catch(UnsupportedEncodingException UEEx)
            {
                logger.log(Level.ERROR, UEEx.getMessage());
            }
            logger.log(Level.DEBUG, message);
            SolrQueryResponse result = performUpdateRequest(plugin_handler, getParams(), message);
            //TODO: allow for the RPC round trip.
            //also allow for failures.
        }

        /**
        * Extract the parameters from the custom headers, if any have been added.
        * @return
        */
        private Map<String, String[]> getParams(){
            Map<String,Object> headers = delivery.getProperties().getHeaders();

            Map<String, String[]> params = new HashMap<String, String[]>();
            if (headers != null){
                Set<String> keys = headers.keySet();
                for (String key: keys){
                    Object value = headers.get(key);
                    params.put(key, new String[]{value.toString()});
                }
            }
            return params;
        }
    }
}
