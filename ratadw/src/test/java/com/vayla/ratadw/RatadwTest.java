package com.vayla.ratadw;

import software.amazon.awscdk.core.App;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.io.IOException;
import static org.junit.Assert.assertThat;

public class RatadwTest {
    private final static ObjectMapper JSON =
        new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, true);

    @Test
    public void testStack() throws IOException {
        App app = new App();
        RatadwExtraStack stack = new RatadwExtraStack(app, "test");

        // synthesize the stack to a CloudFormation template and compare against
        // a checked-in JSON file.
        JsonNode actual = JSON.valueToTree(app.synth().getStackArtifact(stack.getArtifactId()).getTemplate());
        // Check that at least one lambda exists
        assertThat(actual.toString(), CoreMatchers.containsString("AWS::Lambda::Function"));
    }
}
