package com.vayla.Lambdas.eventpasser;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import java.io.OutputStream;
import java.io.InputStream;
/**
 * TODO Placeholder for transfering event to stepfunction
 */


public class EventPass 
implements RequestHandler<Object, String> {
    public String handleRequest(Object input, Context context) {
      String data= input != null ? input.toString() : "{}";  
      context.getLogger().log("Input: " + input);
        return "Hello World - " + data;
    }
}
