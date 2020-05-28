package com.vayla;

import javax.swing.TransferHandler;

import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;
import software.amazon.awscdk.services.ec2.*;
import software.amazon.awscdk.services.transfer.*;
import software.amazon.awscdk.services.transfer.CfnServer.EndpointDetailsProperty;
import software.amazon.awscdk.services.ec2.*;
import software.amazon.awscdk.services.ecs.*;
import software.amazon.awscdk.services.ecs.patterns.*;
import software.amazon.awscdk.services.ecr.assets.DockerImageAsset;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.iam.*;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

public class LamStack extends Stack {
    public LamStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public LamStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        Vpc vpc = Vpc.Builder.create(this, "Lam-VPC").maxAzs(2).build();

        final DockerImageAsset asset = DockerImageAsset.Builder.create(this, "Lam-docker-image")
                .directory(new File(".." + File.separator, "").toString()).exclude(Arrays.asList("node_modules", ".git", "cdk.out", "cdk")).build();

        final Cluster cluster = Cluster.Builder.create(this, "Lam-cluster").vpc(vpc).build();

        final ServicePrincipal servicePrincible = ServicePrincipal.Builder.create("ecs-tasks.amazonaws.com").build();

        final Role executionRole = Role.Builder.create(this, "LAM-ExecutionRole").roleName("ExecutionRole")
                .assumedBy(servicePrincible).build();

        final Role taskRole = Role.Builder.create(this, "LAM-task").roleName("taskRole").assumedBy(servicePrincible)
                .build();

        final FargateTaskDefinition tasdef = FargateTaskDefinition.Builder.create(this, "Lam-container-taskdefinition")
                .cpu(512).memoryLimitMiB(1024).executionRole(executionRole).taskRole(taskRole).build();

        final Policy containerTaskPolicy = Policy.Builder.create(this, "LAM-Container-taskPolicy")
                .roles(Arrays.asList(taskRole)).build();

               

        final PolicyStatement containerStatement = PolicyStatement.Builder.create()
                .actions(Arrays.asList("ecr:GetAuthorizationToken", "ecr:BatchCheckLayerAvailability",
                        "ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage", "logs:CreateLogStream", "logs:PutLogEvents"))
                .resources(Arrays.asList("*")).effect(Effect.ALLOW).build();

        final Policy containerExecutionPolicy = Policy.Builder.create(this, "LAM-Container-ExecutionPolicy")
                .roles(Arrays.asList(executionRole)).build();
        containerExecutionPolicy.addStatements(containerStatement);

        //TODO following is bad practice it should define S3 buckets it actually uses and what operations are needed instead of allowing all to all
        final PolicyStatement containerTaskStatement = PolicyStatement.Builder.create().actions(Arrays.asList("s3:*")) 
                .effect(Effect.ALLOW).resources(Arrays.asList("*")).build();
                
        containerTaskPolicy.addStatements(containerTaskStatement);


        ContainerDefinitionOptions containerOptions = ContainerDefinitionOptions.builder()
        // .entryPoint(Arrays.asList("java"))
        .logging(LogDriver.awsLogs(AwsLogDriverProps.builder()
                        .logGroup(LogGroup.Builder.create(this, "LogGroup")
                                        .logGroupName("LAM--farg-loggroup-"
                                                        )
                                        .build())
                        .streamPrefix("lam-fargate-logs").build()))
        .cpu(256).memoryReservationMiB(512)
        .image(ContainerImage.fromRegistry(asset.getImageUri()))
        .build();


        String containerName=tasdef.addContainer("container", containerOptions).getContainerName();
    }


}
