package com.vayla.lambda.velho.collector;

import java.net.MalformedURLException;
import java.net.URL;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * A simple test harness for locally invoking your Lambda function handler.
 */
@RunWith(MockitoJUnitRunner.class)
public class LambdaFunctionHandlerTest {

    @Mock
    private AmazonS3 s3Client;
    @Mock
    private S3Object s3Object;

    @Captor
    private ArgumentCaptor<GetObjectRequest> getObjectRequest;

    @Test
    public void testLambdaFunctionHandler() throws MalformedURLException {
        String velhourl = "http://latauspalvelu.stg.velho.vayla.fi/viimeisin/varustetiedot/kaiteet.json";
        URL endpoint = new URL(velhourl);
        String output = LambdaFunctionHandler.getS3KeyFromURL(endpoint);

        // TODO: validate output here if needed.
        Assert.assertNotNull(output);
    }
}
