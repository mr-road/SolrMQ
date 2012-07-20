package org.apache.solr.handler.ext;

import org.apache.log4j.Level;
import org.junit.*;
import static org.junit.Assert.*;

/**
 * @author jatherton
 */
public class MessageDecoderTest {
    
    public MessageDecoderTest() {
    }
    @Test
    public void testUTF_8Decoding() throws Exception {
       
        String expResult = "{\"add\":[{\"id\"=\"ma_123_34\"}]}";
        
        String msg = "{\"add\":[{\"id\"=\"ma_123_34\"}]}";
        byte[] message = msg.getBytes("UTF-8");
       
        String result = new MessageDecoder().Decode(message);
        assertEquals(expResult, result); 
    }
}
