package com.cgi.lambda.exceltocsv;

// import java.io.IOException;
// import java.nio.ByteBuffer;
// import java.util.HashMap;
// import java.util.Map;
// import java.util.Properties;
// import java.util.ArrayList;
// import java.util.List;

//import javax.swing.text.AbstractDocument.Content;

// import software.amazon.awssdk.regions.Region;
// import com.amazonaws.services.lambda.AWSLambda;
// import com.amazonaws.services.lambda.AWSLambdaClientBuilder;
// import com.amazonaws.services.lambda.model.InvokeResult;
// import com.fasterxml.jackson.databind.ObjectMapper;

// import software.amazon.awssdk.services.lambda.model.InvocationType;
// import software.amazon.awssdk.services.lambda.model.LogType;
// import software.amazon.awssdk.core.SdkBytes;
// import software.amazon.awssdk.services.lambda.LambdaClient;
// import software.amazon.awssdk.services.lambda.model.InvokeResponse;
// import software.amazon.awssdk.services.ses.SesClient;
// import software.amazon.awssdk.services.lambda.model.InvokeRequest;


// import software.amazon.awssdk.services.ses.model.SendEmailRequest;
// import software.amazon.awssdk.services.ses.model.Message;
// import software.amazon.awssdk.services.ses.model.Destination;
// import software.amazon.awssdk.services.ses.model.Body;
// import software.amazon.awssdk.services.ses.model.Content;


// import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
// import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
// import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
// import com.fasterxml.jackson.core.type.TypeReference;


public class ErrorMail {

    private String subject = "";
    private String message = "";

    // private String mail_to = "";
    // private String mail_from = "";
    // private String[] mail_cc = null;
    // private String[] reply_to = null;
    //private String secret_name = System.getenv("secret_name");

    private String baseName = "";

    public ErrorMail() {
        //getSecretValues(secret_name);

    }
    
    public ErrorMail(String subject) {
        //getSecretValues(secret_name);
        this.subject = subject;
    }
/* 
    public boolean invokeMail() {

        if (this.message.length() > 0){
            // Construct a JSON payload string with message and subject
        String jsonPayload = "{\"message\":\"" + message + "\",\"subject\":\"" + subject + "\"}";

        // Create an AWS Lambda client
        LambdaClient lambdaClient = LambdaClient.builder()
                .region(Region.EU_WEST_1) // Replace with your desired AWS region
                .build();

        // Configure the request to invoke the Python Lambda function
        InvokeRequest invokeRequest = InvokeRequest.builder()
                .functionName("YourPythonLambdaFunctionName") // Replace with the name of your Python Lambda function
                .invocationType(InvocationType.EVENT)  // Change to InvocationType.REQUEST_RESPONSE if you need a response
                .logType(LogType.TAIL)
                .payload(SdkBytes.fromUtf8String(jsonPayload))
                .build();

        // Invoke the Python Lambda function
        InvokeResponse invokeResponse = lambdaClient.invoke(invokeRequest);

        // Handle the response if using InvocationType.REQUEST_RESPONSE
        // String responsePayload = invokeResponse.payload().asUtf8String();
        // Handle the responsePayload as needed

        return true;
        } 
        else{
            return false;
        }
              
    }
*/
    public boolean add(String s){
        if (s.length() > 0){
            if (this.message.length() > 0){
                this.message += "\n" + s;
            }
            else{
                this.message = "Huomenta! \n\nAutomaattinen virheentarkistus on löytänyt tiedostosta \"" + 
                this.baseName + "\" seuraavat virheet:\n";
                this.message += "\n" + s;
            } 
            return true;
        }
        else
        return false;
    }

    public String getMessage(){
        return this.message;
    }

    public boolean setSubject(String s){
        if (s.length() > 0){
            this.subject = s;
            return true;
        }
        else
        return false;
    }

    public void setBaseName(String baseName) {
		this.baseName = baseName;
	}
/* 
    public boolean sendMail() {
        if (this.message.length() > 0) {
            SesClient sesClient = SesClient.builder().region(Region.EU_CENTRAL_1).build();

            // Create the email request
            SendEmailRequest emailRequest = SendEmailRequest.builder()
                    .source(this.mail_from)
                    .destination(Destination.builder()
                        .toAddresses(this.mail_to)
                        .ccAddresses(this.mail_cc).build())
                    .message(Message.builder()
                            .subject(Content.builder().data(this.subject).build())
                            .body(Body.builder().text(Content.builder().data(this.message).build()).build())
                            .build())
                    .replyToAddresses(this.reply_to)
                    .build();

            // Send the email
            sesClient.sendEmail(emailRequest);

            return true;
        } else {
            return false;
        }
    }
    */
    public void sendSns(String arn){
        ErrorSns sns = new ErrorSns();
        sns.setSnsArn(arn);
        sns.setMessage(this.message);
        sns.setSubject(this.subject);
        sns.sendSns();
    }
    /*
    public boolean /*Map<String, String> getSecretValues(String secretName) {
        Map<String, String> secretMap = new HashMap<>();

        // Create a Secrets Manager client
        SecretsManagerClient secretsManagerClient = SecretsManagerClient.builder()
                .region(Region.EU_CENTRAL_1) // Replace with your desired region
                .build();

        try {
            // Create a GetSecretValueRequest
            GetSecretValueRequest secretValueRequest = GetSecretValueRequest.builder()
                    .secretId(secretName)
                    .build();

            // Retrieve the secret value
            GetSecretValueResponse secretValueResponse = secretsManagerClient.getSecretValue(secretValueRequest);

            // Check if the secret is a string
            if (secretValueResponse.secretString() != null) {
                String secretString = secretValueResponse.secretString();
                // Parse the JSON content in the secretString using Jackson
                ObjectMapper objectMapper = new ObjectMapper();
                secretMap = objectMapper.readValue(secretString, new TypeReference<Map<String, String>>() {});
            }
        } catch (Exception e) {
            // Handle any exceptions or errors here
            e.printStackTrace();
            return false;
        } finally {
            // Close the SecretsManagerClient when done
            secretsManagerClient.close();
        }

        this.mail_to = secretMap.get("mail_to");
        this.mail_cc = secretMap.get("mail_cc").split(";");
        this.mail_from = secretMap.get("mail_from");
        this.reply_to = secretMap.get("reply_to").split(";");

        return true;
        //return secretMap;
    }
    */
}