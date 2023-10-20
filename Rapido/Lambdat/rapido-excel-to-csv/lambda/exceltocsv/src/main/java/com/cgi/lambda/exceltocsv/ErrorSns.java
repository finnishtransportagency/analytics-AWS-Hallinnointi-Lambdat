package com.cgi.lambda.exceltocsv;

import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

public class ErrorSns {

    // Initialize the SNS client
    private AmazonSNS snsClient;

    private String snsArn;
    private String message;
    private String subject;

    public ErrorSns() {
        snsClient = AmazonSNSClientBuilder.standard().build();
    }

    public void setSnsArn(String arn) {
        this.snsArn = arn;
    }

    public void setMessage(String msg) {
        this.message = msg;
    }

    public void setSubject(String subj) {
        this.subject = subj;
    }

    public String sendSns() {
        // Create the publish request
        PublishRequest publishRequest = new PublishRequest()
                .withTopicArn(snsArn)
                .withMessage(message)
                .withSubject(subject);

        // Publish the message to the SNS topic
        PublishResult publishResult = snsClient.publish(publishRequest);

        // You can log or return information about the publish result if needed
        // System.out.println("Message sent with MessageId: " + publishResult.getMessageId());

        return "Success"; // Return a response
    }
}
