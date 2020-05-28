package com.vayla;


import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;
import software.amazon.awscdk.services.ec2.*;
import software.amazon.awscdk.services.events.*;
import software.amazon.awscdk.services.events.targets.EcsTask;
import software.amazon.awscdk.services.ecs.*;
import software.amazon.awscdk.services.ecr.assets.DockerImageAsset;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.iam.*;
import java.io.File;
import java.util.*;
import java.util.Arrays;

public class LamStack extends Stack {
        public LamStack(final Construct scope, final String id) {
                this(scope, id, null);
        }

        public LamStack(final Construct scope, final String id, final StackProps props) {
                super(scope, id, props);

                Vpc vpc = Vpc.Builder.create(this, "Lam-VPC").maxAzs(2).build();

                final DockerImageAsset asset = DockerImageAsset.Builder.create(this, "Lam-docker-image")
                                .directory(new File(".." + File.separator, "").toString())
                                .exclude(Arrays.asList("node_modules", ".git", "cdk.out", "cdk")).build();

                final Cluster cluster = Cluster.Builder.create(this, "Lam-cluster").vpc(vpc).build();

                final ServicePrincipal servicePrincible = ServicePrincipal.Builder.create("ecs-tasks.amazonaws.com")
                                .build();

                final Role executionRole = Role.Builder.create(this, "LAM-ExecutionRole").roleName("ExecutionRole")
                                .assumedBy(servicePrincible).build();

                final Role taskRole = Role.Builder.create(this, "LAM-task").roleName("taskRole")
                                .assumedBy(servicePrincible).build();

                final FargateTaskDefinition tasdef = FargateTaskDefinition.Builder
                                .create(this, "Lam-container-taskdefinition").cpu(512).memoryLimitMiB(1024)
                                .executionRole(executionRole).taskRole(taskRole).build();

                final Policy containerTaskPolicy = Policy.Builder.create(this, "LAM-Container-taskPolicy")
                                .roles(Arrays.asList(taskRole)).build();

                final PolicyStatement containerStatement = PolicyStatement.Builder.create()
                                .actions(Arrays.asList("ecr:GetAuthorizationToken", "ecr:BatchCheckLayerAvailability",
                                                "ecr:GetDownloadUrlForLayer", "ecr:BatchGetImage",
                                                "logs:CreateLogStream", "logs:PutLogEvents"))
                                .resources(Arrays.asList("*")).effect(Effect.ALLOW).build();

                final Policy containerExecutionPolicy = Policy.Builder.create(this, "LAM-Container-ExecutionPolicy")
                                .roles(Arrays.asList(executionRole)).build();
                containerExecutionPolicy.addStatements(containerStatement);

                // TODO following is bad practice it should define S3 buckets it actually uses
                // and what operations are needed instead of allowing all to all,
                final PolicyStatement policyStatementForS3 = PolicyStatement.Builder.create()
                                .actions(Arrays.asList("s3:*")).effect(Effect.ALLOW).resources(Arrays.asList("*"))
                                .build();

                containerTaskPolicy.addStatements(policyStatementForS3);

                ContainerDefinitionOptions containerOptions = ContainerDefinitionOptions.builder()
                                .logging(LogDriver.awsLogs(AwsLogDriverProps.builder()
                                                .logGroup(LogGroup.Builder.create(this, "LogGroup")
                                                                .logGroupName("LAM--farg-loggroup-").build())
                                                .streamPrefix("lam-fargate-logs").build()))
                                .cpu(256).memoryReservationMiB(512)
                                .image(ContainerImage.fromRegistry(asset.getImageUri())).build();

                tasdef.addContainer("container", containerOptions).getContainerName();

                vpc.selectSubnets();

                ArrayList<String> subs = new ArrayList<String>();
                for (ISubnet subnet : vpc.getPrivateSubnets()) {
                        subs.add(subnet.getSubnetId());
                }

                vpc.selectSubnets(SubnetSelection.builder().subnetType(SubnetType.PRIVATE).build());

                final PolicyStatement EndpointPolicyForS3 = PolicyStatement .Builder.create()
                .actions(Arrays.asList("s3:*")).effect(Effect.ALLOW).resources(Arrays.asList("*"))
                .build();

                EndpointPolicyForS3.addAnyPrincipal();

                GatewayVpcEndpoint s3VPCEndP = GatewayVpcEndpoint.Builder.create(this, "S3EndPoint")
                                .service(GatewayVpcEndpointAwsService.S3).vpc(vpc)
                                .subnets(Arrays.asList(
                                                SubnetSelection.builder().subnetType(SubnetType.PRIVATE).build()))
                                .build();
                s3VPCEndP.addToPolicy(EndpointPolicyForS3);


                Rule lamTrigger = Rule.Builder.create(this, "LAM-Fargate-Trigger")
                                .schedule(Schedule.cron(CronOptions.builder().hour("23").build())).build();

                EcsTask task = EcsTask.Builder.create().cluster(cluster).taskCount(1).taskDefinition(tasdef)
                                .subnetSelection(SubnetSelection.builder().subnetType(SubnetType.PRIVATE).build())
                                .build();
                task.bind(lamTrigger);

                lamTrigger.addTarget(task);


        }

}
