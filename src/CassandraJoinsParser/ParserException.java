/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CassandraJoinsParser;

import java.security.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 * @author Alex
 */
public class ParserException extends Exception
{
    private static final long serialVersionUID = 7526472295622776147L;
    private static final String EXCEPTION_HOLDER = "[Parser Exception]";
    private static final String ERROR_HOLDER = "ERROR";
    private String message;
    private Throwable ex;
    
    public ParserException(){}
    
    public ParserException(String message)
    {
        super(message);
        complementExceptionMessage(message);
    }
    
    public ParserException(String message, Throwable ex)
    {        
        super(message, ex);
        this.ex = ex;
        
        complementExceptionMessage(message);
    }
    
    private void complementExceptionMessage(String message)
    {
        SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss.SSS");
        Date date = new Date();
        String timeStamp = sdf.format(date.getTime());
        String finalMessage;
        
        finalMessage = timeStamp + " " + EXCEPTION_HOLDER + " " + ERROR_HOLDER + " " + message;
        
        if (ex != null)
        {
            String exStackTrace = finalMessage.concat("\n");
            
            for (StackTraceElement elem : ex.getStackTrace())
            {
                exStackTrace = exStackTrace.concat(elem.toString() + "\n");
            }
            
            finalMessage = exStackTrace;
        }
        
        this.message = finalMessage;
    }
    
    @Override
    public String getMessage()
    {
        return this.message;
    }
}
