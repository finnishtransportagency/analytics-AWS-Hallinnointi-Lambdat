package com.vayla;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import software.amazon.awscdk.core.App;
import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;
import software.amazon.awscdk.services.codebuild.BuildEnvironment;
import software.amazon.awscdk.services.codebuild.BuildSpec;
import software.amazon.awscdk.services.codebuild.LinuxBuildImage;
import software.amazon.awscdk.services.codebuild.PipelineProject;
import software.amazon.awscdk.services.codepipeline.Artifact;
import software.amazon.awscdk.services.codepipeline.Pipeline;
import software.amazon.awscdk.services.codepipeline.StageProps;
import software.amazon.awscdk.services.codepipeline.actions.CloudFormationCreateUpdateStackAction;
import software.amazon.awscdk.services.codepipeline.actions.CodeBuildAction;
import software.amazon.awscdk.services.codepipeline.actions.GitHubSourceAction;
import software.amazon.awscdk.services.lambda.CfnParametersCode;
import software.amazon.awscdk.services.secretsmanager.ISecret;
import software.amazon.awscdk.services.secretsmanager.Secret;
import software.amazon.awscdk.services.secretsmanager.SecretAttributes;

public class VelhoPipelineStack extends Stack {
	public VelhoPipelineStack(final App scope, final String id) {
		this(scope, id, null);
	}

	public VelhoPipelineStack(final App scope, final String id, final StackProps props) {
		super(scope, id, props);

		// Velho github access token from AWS Secrets manager
		// accessToken:<somestring>
		ISecret githubSecret = Secret.fromSecretAttributes(this, "GitHubSecret", SecretAttributes.builder()
				.secretArn("arn:aws:secretsmanager:eu-central-1:593223377027:secret:dev/vayla/github-OXfFeD").build());

		/* Build phase projects */
		
		// build project for the main cdk project
		PipelineProject cdkBuild = PipelineProject.Builder.create(this, "CDKBuild")
				.buildSpec(BuildSpec.fromObject(new HashMap<String, Object>() {
					{
						put("version", "0.2");
						put("phases", new HashMap<String, Object>() {
							{
								put("build", new HashMap<String, Object>() {
									{
										put("commands", Arrays.asList("cd Velho-pipeline", "mvn package", "cdk synth -- o dist"));
									}
								});
							}
						});
						put("artifacts", new HashMap<String, String>() {
							{
								put("base-directory", "dist");
							}
						});
						put("files", Arrays.asList("VelhoAnalyticsStack.template.json"));
					}
				}))
				.environment(BuildEnvironment.builder().buildImage(LinuxBuildImage.AMAZON_LINUX_2).build()) //  UBUNTU_14_04_NODEJS_10_14_1
				.build();
		
		// build project to run 'cdk deploy'
		PipelineProject cdkDeploy = PipelineProject.Builder.create(this, "CDKDeploy")
				.buildSpec(BuildSpec.fromObject(new HashMap<String, Object>() {
					{
						put("version", "0.2");
						put("phases", new HashMap<String, Object>() {
							{
								put("deploy", new HashMap<String, Object>() {
									{
										put("commands", Arrays.asList("cd Velho-pipeline", "cdk deploy"));
									}
								});
							}
						});
					}
				}))
				.environment(BuildEnvironment.builder().buildImage(LinuxBuildImage.AMAZON_LINUX_2).build()) //  UBUNTU_14_04_NODEJS_10_14_1
				.build();

		// build project to run 'mvn package' for lambda sub projects
//		PipelineProject lambdaBuild = PipelineProject.Builder.create(this, "LambdaBuild")
//				.buildSpec(BuildSpec.fromObject(new HashMap<String, Object>() {
//					{
//						put("version", "0.2");
//						put("phases", new HashMap<String, Object>() {
//							{
//								put("build velhocollector", new HashMap<String, List<String>>() {
//									{
//										put("commands", Arrays.asList("cd lambdas", "cd velhocollector", "mvn package"));
//									}
//								});
//								put("build velhodataloader", new HashMap<String, List<String>>() {
//									{
//										put("commands", Arrays.asList("cd lambdas", "cd velhodataloader", "mvn package"));
//									}
//								});
//								put("build velhometadata", new HashMap<String, List<String>>() {
//									{
//										put("commands", Arrays.asList("cd lambdas", "cd velhometadata", "mvn package"));
//									}
//								});
//								put("build velhometadataconverter", new HashMap<String, List<String>>() {
//									{
//										put("commands", Arrays.asList("cd lambdas", "cd velhometadataconverter", "mvn package"));
//									}
//								});
//
//							}
//						});
//						put("artifacts", new HashMap<String, Object>() {
//							{
//								put("base-directory", "lambdas");
//								//put("files", Arrays.asList("index.js", "node_modules/**/*"));
//								// files could pose a problem since the main project (pom) references
//								// the lambda jars from the relative subdirs
//							}
//						});
//					}
//				}))
//				.environment(BuildEnvironment.builder().buildImage(LinuxBuildImage.UBUNTU_14_04_NODEJS_10_14_1).build())
//				.build();
		
		
		/* Outputs for different projects */
		
		// checked out code from github
		Artifact sourceOutput = new Artifact();
		
		// the artifacts from the main cdk build process
		// aka cloudformation json, and deployable zips
		Artifact cdkBuildOutput = new Artifact("CdkBuildOutput");
		
		// artifacts from lambda build project that have to be generated first
		// aka lambdas packaged in jars
		// Artifact lambdaBuildOutput = new Artifact("LambdaBuildOutput");
		
		
		// CodePipeline
		// with stages for 1) source 2) build 3) deploy
		Pipeline.Builder.create(this, "Pipeline")
        .stages(Arrays.asList(
            StageProps.builder()
                .stageName("Source")
                .actions(Arrays.asList(
                	GitHubSourceAction.Builder.create() 
                		.actionName("GithubSource")
                		.repo("analytics-AWS-Hallinnointi-Lambdat")
                		.branch("VelhoDev")
                		.owner("kettunju")
                		.oauthToken(githubSecret.secretValueFromJson("oauth-token")) //getSecretValue())
                		.output(sourceOutput)
                		.build()))
                .build(),
            StageProps.builder()
                .stageName("Build")
                .actions(Arrays.asList(
// Lambdas will have to be packaged first
//                    CodeBuildAction.Builder.create()
//                        .actionName("Lambda_Build")
//                        .project(lambdaBuild)
//                        .input(sourceOutput)
//                        .outputs(Arrays.asList(lambdaBuildOutput)).build(),
                    CodeBuildAction.Builder.create()
                        .actionName("Cdk_Build")
                        .project(cdkBuild)
                        .input(sourceOutput)
                        .outputs(Arrays.asList(cdkBuildOutput))
                        .build()))
                .build(),
            StageProps.builder()
                .stageName("Deploy")
                .actions(Arrays.asList(
                		CodeBuildAction.Builder.create()
                        .actionName("Cdk_Deploy")
                        .project(cdkDeploy)
                        .input(cdkBuildOutput)
                        .build()))
                .build()))
        .build();
		
	}
}
