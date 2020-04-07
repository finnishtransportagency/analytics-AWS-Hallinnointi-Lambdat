package com.vayla;

import software.amazon.awscdk.core.App;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.vayla.VaylaTalvimerenkulkuStack;

import org.hamcrest.CoreMatchers;
import org.junit.Test;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class VaylaTalvimerenkulkuTest {
    private final static ObjectMapper JSON =
        new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, true);

    @Test
    public void testStack() throws IOException {
        App app = new App();
        VaylaTalvimerenkulkuStack stack = new VaylaTalvimerenkulkuStack(app, "test");

        // loytyyko edes yksi lambda
        JsonNode actual = JSON.valueToTree(app.synth().getStackArtifact(stack.getArtifactId()).getTemplate());
        
        assertThat(actual.toString(), CoreMatchers.containsString("AWS::Lambda::Function"));

    }
}
