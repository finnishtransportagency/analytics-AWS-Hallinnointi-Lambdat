package com.vayla;

import java.util.Arrays;
import java.util.List;
import java.util.HashMap;

import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.SecretValue;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;

import software.amazon.awscdk.services.codebuild.BuildEnvironment;
import software.amazon.awscdk.services.codebuild.BuildSpec;
import software.amazon.awscdk.services.codebuild.LinuxBuildImage;
import software.amazon.awscdk.services.codebuild.PipelineProject;

import software.amazon.awscdk.services.codecommit.Repository;

import software.amazon.awscdk.services.codepipeline.Artifact;
import software.amazon.awscdk.services.codepipeline.StageProps;
import software.amazon.awscdk.services.codepipeline.Pipeline;

import software.amazon.awscdk.services.codepipeline.actions.CloudFormationCreateUpdateStackAction;
import software.amazon.awscdk.services.codepipeline.actions.CodeBuildAction;
import software.amazon.awscdk.services.codepipeline.actions.CodeCommitSourceAction;
import software.amazon.awscdk.services.codepipeline.actions.GitHubSourceAction;
import software.amazon.awscdk.services.events.targets.CodeBuildProject;
import software.amazon.awscdk.services.lambda.CfnParametersCode;
import software.amazon.awscdk.services.secretsmanager.Secret;
import software.amazon.awscdk.services.secretsmanager.SecretAttributes;

/*
 * TODO: velho cdk ja lambda pipelinet
 * https://docs.aws.amazon.com/cdk/latest/guide/codepipeline_example.html#w84aac15c11c15c17b5
 */
public class PipelineStack extends Stack {
    // alternate constructor for calls without props. lambdaCode is required. 
    public PipelineStack(final Construct scope, final String id, final CfnParametersCode lambdaCode) {
        this(scope, id, null, lambdaCode);
    }
    
    @SuppressWarnings("serial")
    public PipelineStack(final Construct scope, final String id, final StackProps props, final CfnParametersCode lambdaCode) {
        super(scope, id, props);
        
        Secret githubSecret = (Secret)Secret.fromSecretAttributes(this, "GitHubSecret", SecretAttributes.builder()
                .secretArn("arn:aws:secretsmanager:eu-central-1:426182641979:secret:velho/github-NRRYEN")
                .build());

        PipelineProject cdkBuild = PipelineProject.Builder.create(this, "CDKBuild") 
                    .buildSpec(BuildSpec.fromObject(new HashMap<String, Object>() {{
                        put("version", "0.2");
                        put("phases", new HashMap<String, Object>() {{
                            put("build", new HashMap<String, Object>() {{
                                put("commands", Arrays.asList("npm run build", 
                                                              "npm run cdk synth -- o dist"));
                            }});
                        }});
                        put("artifacts", new HashMap<String, String>() {{
                            put("base-directory", "dist");
                        }});
                        put("files", Arrays.asList("LambdaStack.template.json"));
                    }}))
                    .environment(BuildEnvironment.builder().buildImage(
                            LinuxBuildImage.UBUNTU_14_04_NODEJS_10_14_1).build())
                    .build();

        PipelineProject lambdaBuild = PipelineProject.Builder.create(this, "LambdaBuild") 
                    .buildSpec(BuildSpec.fromObject(new HashMap<String, Object>() {{
                        put("version", "0.2");
                        put("phases", new HashMap<String, Object>() {{ 
                            put("build eventpasser", new HashMap<String, List<String>>() {{
                                put("commands", Arrays.asList("cd lambdas", "cd eventpasser","mvn package"));
                            }});
                            put("build velhometadata", new HashMap<String, List<String>>() {{
                                put("commands", Arrays.asList("cd lambdas", "cd velhometadata", "mvn package"));
                            }});

                        }});
                        put("artifacts", new HashMap<String, Object>() {{
                            put("base-directory", "lambdas");
                            put("files", Arrays.asList("index.js", "node_modules/**/*"));
                        }});
                    }}))
                    .environment(BuildEnvironment.builder().buildImage(
                            LinuxBuildImage.UBUNTU_14_04_NODEJS_10_14_1).build())
                    .build();

        Artifact sourceOutput = new Artifact();
        Artifact cdkBuildOutput = new Artifact("CdkBuildOutput");
        Artifact lambdaBuildOutput = new Artifact("LambdaBuildOutput");

        Pipeline.Builder.create(this, "Pipeline")
                .stages(Arrays.asList(
                    StageProps.builder()
                        .stageName("Source")
                        .actions(Arrays.asList(
                        		//TODO: fill in github stuff
                        		// https://docs.aws.amazon.com/cdk/api/latest/docs/aws-codebuild-readme.html
                        	GitHubSourceAction.Builder.create() 
                        		.actionName("GithubSource")
                        		.repo("finnishtransportagency/analytics-AWS-Hallinnointi-Lambdat/Velho-analytics")
                        		.branch("VelhoDev")
                        		.oauthToken(githubSecret.getSecretValue())
                        		.output(sourceOutput)
                        		.build()))
                        .build(),
                    StageProps.builder()
                        .stageName("Build")
                        .actions(Arrays.asList(
                            CodeBuildAction.Builder.create()
                                .actionName("Lambda_Build")
                                .project(lambdaBuild)
                                .input(sourceOutput)
                                .outputs(Arrays.asList(lambdaBuildOutput)).build(),
                            CodeBuildAction.Builder.create()
                                .actionName("Lambda_Build")
                                .project(cdkBuild)
                                .input(sourceOutput)
                                .outputs(Arrays.asList(cdkBuildOutput))
                                .build()))
                        .build(),
                    StageProps.builder()
                        .stageName("Deploy")
                        .actions(Arrays.asList(
                             CloudFormationCreateUpdateStackAction.Builder.create()
                                         .actionName("Lambda_CFN_Deploy")
                                         .templatePath(cdkBuildOutput.atPath("LambdaStack.template.json"))
                                         .adminPermissions(true)
                                         .parameterOverrides(lambdaCode.assign(lambdaBuildOutput.getS3Location()))
                                         .extraInputs(Arrays.asList(lambdaBuildOutput))
                                         .build()))
                        .build()))
                .build();
    }
}