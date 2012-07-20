/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.solr.handler.ext;

import java.io.UnsupportedEncodingException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 *
 * @author jamesa
 */
public class  MessageDecoder
{
    public String Decode  (byte[] message)  throws UnsupportedEncodingException
    {
        return new String(message, "UTF-8");
    }
}
