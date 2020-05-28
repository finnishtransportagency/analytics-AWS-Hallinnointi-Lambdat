package com.vayla.lambda.velho.metadata.converter;

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
import com.vayla.lambda.velho.metadata.converter.ade.NimikeCsvHelper;

/**
 * A simple test harness for locally invoking your Lambda function handler.
 */
@RunWith(MockitoJUnitRunner.class)
public class LambdaFunctionHandlerTest {

    private final String HEADERS = "\"NIMI\",\"KOODI\",\"OTSIKKO\",\"TR_MAPPAUKSET\"";

    @Mock
    private AmazonS3 s3Client;
    @Mock
    private S3Object s3Object;

    @Captor
    private ArgumentCaptor<GetObjectRequest> getObjectRequest;


    @Test
    public void testNimikeCsvHelper() {

        String output = NimikeCsvHelper.getHeadersForCsv();

        // TODO: validate output here if needed.
        Assert.assertEquals(HEADERS, output);
    }
}
