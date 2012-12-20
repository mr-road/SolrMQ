package org.apache.solr.handler.ext;

import com.rabbitmq.client.QueueingConsumer;
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
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.request.SolrRequestHandler;
import org.apache.solr.response.SolrQueryResponse;

    /**
    * Worker thread. This is spawned for each message consumed.
    * @author rnoble
    * @author jatherton
    */
    public class QueueUpdateWorker extends Thread{ 
        QueueingConsumer.Delivery delivery;
        String plugin_handler;
        protected SolrCore core;
        
        Logger  logger = Logger.getLogger("org.apache.solr.handler.ext.QueueUpdateWorker");
        
        public QueueUpdateWorker(QueueingConsumer.Delivery delivery, String plugin_handler){
            super();
            this.delivery = delivery;
            this.plugin_handler = plugin_handler;
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
    * Performs the actual update request
    * @param handler - name of the handler, like /update or /update/json. Should probably be loaded.
    * @param params - the parameters, these can be parsed as custom message headers
    * @param message - the actual message, at present only strings are allowed.
    * @return SolrQueryResponse - returns the actual response. Check the Exception to handle faults
    */
    private SolrQueryResponse performUpdateRequest(String handler, Map<String, String[]> params, String message){

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
