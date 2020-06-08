package com.vayla;

import software.amazon.awscdk.core.App;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.Test;

import java.io.IOException;
import org.hamcrest.CoreMatchers;

import static org.junit.Assert.assertThat;

public class LamTest {
    private final static ObjectMapper JSON =
        new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, true);

    @Test
    public void testStack() throws IOException {
        App app = new App();
        LamStack stack = new LamStack(app, "test");

        // synthesize the stack to a CloudFormation template and compare against
        // a checked-in JSON file.
        JsonNode actual = JSON.valueToTree(app.synth().getStackArtifact(stack.getArtifactId()).getTemplate());
        //assertEquals(new ObjectMapper().createObjectNode(), actual);
        //Loytyyko ecs:lle vpc
        assertThat(actual.toString(), CoreMatchers.containsString("AWS::EC2::VPC"));
        //Loytyyko ecs-klusteri
        assertThat(actual.toString(), CoreMatchers.containsString("AWS::ECS::Cluster"));
        //Loytyyko ecs-task
        assertThat(actual.toString(), CoreMatchers.containsString("AWS::ECS::TaskDefinition"));
        //Loytyyko 
    }
}
