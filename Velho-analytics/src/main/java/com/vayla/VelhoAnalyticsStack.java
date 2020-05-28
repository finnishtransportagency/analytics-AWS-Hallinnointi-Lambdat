package com.vayla;

//import com.sun.tools.javac.util.Dependencies;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.Duration;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.BucketProps;
import software.amazon.awscdk.services.s3.NotificationKeyFilter;
import software.amazon.awscdk.services.s3.notifications.LambdaDestination;
import software.amazon.awscdk.services.sns.Topic;
import software.amazon.awscdk.services.sqs.Queue;

public class VelhoAnalyticsStack extends Stack {
	public VelhoAnalyticsStack(final Construct parent, final String id) {
		this(parent, id, null);
	}

	public VelhoAnalyticsStack(final Construct parent, final String id, final StackProps props) {
		super(parent, id, props);

		final BucketProps s3BucketProps = BucketProps.builder().encryption(BucketEncryption.S3_MANAGED).versioned(true)
				// .lifecycleRules(lifecycleRules) lets add this later so old files are cleared
				// after defined time
				.build();

		Bucket landingBucket = new Bucket(this, "velholandingbucket", s3BucketProps);
		Bucket workBucket = new Bucket(this, "velhoworkbucket", s3BucketProps);

//		final Function evenPasserLambda = Function.Builder.create(this, "EventPasser")
//				.functionName("VelhoLandingBucketEventPasser").timeout(Duration.minutes(5))
//				.code(Code.fromAsset("lambdas" + File.separator + "eventpasser" + File.separator + "target"
//						+ File.separator + "lambda-java-evenpasser-1.0-SNAPSHOT.jar"))
//				.runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_8)
//				.handler("com.vayla.Lambdas.eventpasser.EventPass").build();

		// Metadataloderin ymparistomuuttujat
		Map<String, String> environment = new HashMap<String, String>();
		environment.put("velhoHost", "api.stg.velho.vayla.fi");
		environment.put("workBucket", workBucket.getBucketName());
		environment.put("metadataprefix", "metadata/");
		environment.put("landingbucket", landingBucket.getBucketName()); // raakadatan landing bucket, josta triggerit kasittely lambdoihin


		final Function getMetadataLambda = Function.Builder.create(this, "MetadataLoaderLambda")
				.functionName("VelhoMetadataLoader").timeout(Duration.minutes(5)).memorySize(1024)
				.code(Code.fromAsset("lambdas" + File.separator + "velhometadata" + File.separator + "target"
						+ File.separator + "velho.metadata-1.0.0.jar"))
				.runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_8).environment(environment)
				.handler("com.vayla.lambda.velho.metadata.LambdaFunctionHandler").build();
		// lisataan kayttooikeudet workbuckettiin
		// TODO: rajatut bucketit?
		getMetadataLambda
				.addToRolePolicy(PolicyStatement.Builder.create()
						.effect(Effect.ALLOW).actions(Arrays.asList("s3:*"))
						// .resources(Arrays.asList(workBucket.getBucketArn()))
						.resources(Arrays.asList("*")).build());
		
		getMetadataLambda
		.addToRolePolicy(PolicyStatement.Builder.create()
				.effect(Effect.ALLOW)
				.actions(Arrays.asList("secretsmanager:*"))
				.resources(Arrays.asList("arn:aws:secretsmanager:eu-central-1:426182641979:secret:VelhoSecrets-pWNDv4")).build());
		
		// Metadataloderin ymparistomuuttujat
		environment = new HashMap<String, String>();
		environment.put("schema_versio", "1");
		environment.put("debug", "true");
		environment.put("workbucket", workBucket.getBucketName());
		environment.put("adeBucket", "file-load-ade-runtime-dev");


		final Function convertMetadataLambda = Function.Builder.create(this, "MetadataConverterLambda")
				.functionName("VelhoMetadataConverter").timeout(Duration.minutes(5)).memorySize(1024)
				.code(Code.fromAsset("lambdas" + File.separator + "velhometadataconverter" + File.separator + "target"
						+ File.separator + "velho.metadata.converter-1.0.0.jar"))
				.runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_8).environment(environment)
				.handler("com.vayla.lambda.velho.metadata.LambdaFunctionHandler").build();
		// lisataan kayttooikeudet workbuckettiin
		// TODO: rajatut bucketit?
		convertMetadataLambda
				.addToRolePolicy(PolicyStatement.Builder.create()
						.effect(Effect.ALLOW).actions(Arrays.asList("s3:*"))
						// .resources(Arrays.asList(workBucket.getBucketArn()))
						.resources(Arrays.asList("*")).build());

		
		// Dataloader ymparistomuuttujat
		// eli pelkka workbucket, jos / kun raakadatan sijainti tulee
		// triggerin s3eventissa
		environment = new HashMap<String, String>();
		environment.put("workbucket", workBucket.getBucketName());
		environment.put("adebucket", "livi-ade-dev-runtime-dagger-notifications");
		environment.put("databucket", "file-load-ade-runtime-dev");
		environment.put("debug", "false");
		
		final Function velhoDataLoderLambda = Function.Builder.create(this, "DataLoaderLambda")
				.functionName("VelhoDataLoader").timeout(Duration.minutes(5)).memorySize(1024)
				.code(Code.fromAsset("lambdas" + File.separator + "velhodataloader" + File.separator + "target"
						+ File.separator + "velho.dataloader-1.0.0.jar"))
				.runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_8).environment(environment)
				.handler("com.vayla.lambda.velho.dataloader.LambdaFunctionHandler").build();

		// lisataan kayttooikeudet kaikkiin bucketteihin
		// todo: rajatut bucketit?
		velhoDataLoderLambda
				.addToRolePolicy(PolicyStatement.Builder.create()
						.effect(Effect.ALLOW)
						.actions(Arrays.asList("s3:*"))
						.resources(Arrays.asList("*")).build());
		
		// AWS SSM velho -> ade mappausten haku
		velhoDataLoderLambda
		.addToRolePolicy(PolicyStatement.Builder.create()
				.effect(Effect.ALLOW)
				.actions(Arrays.asList("secretsmanager:*"))
				.resources(Arrays.asList("arn:aws:secretsmanager:eu-central-1:426182641979:secret:VelhoSecrets-pWNDv4")).build());
		
		
		// Collector (raakadan haku) ymparistomuuttujat
		environment = new HashMap<String, String>();
		environment.put("velhodataurl", "http://latauspalvelu.stg.velho.vayla.fi/viimeisin/varustetiedot/kaiteet.json"); //velho latauspalvelu
		environment.put("landingbucket", landingBucket.getBucketName()); // raakadatan landing bucket, josta triggerit kasittely lambdoihin
		
		final Function velhoCollectorLambda = Function.Builder.create(this, "CollectorLambda")
				.functionName("VelhoCollector").timeout(Duration.minutes(5)).memorySize(1024)
				.code(Code.fromAsset("lambdas" + File.separator + "velhocollector" + File.separator + "target"
						+ File.separator + "velho.collector-1.0.0.jar"))
				.runtime(software.amazon.awscdk.services.lambda.Runtime.JAVA_8).environment(environment)
				.handler("com.vayla.lambda.velho.dataloader.LambdaFunctionHandler").build();
		
		// lisataan kayttooikeudet kaikkiin bucketteihin
		// todo: rajatut bucketit?
		velhoCollectorLambda
				.addToRolePolicy(PolicyStatement.Builder.create()
						.effect(Effect.ALLOW)
						.actions(Arrays.asList("s3:*"))
						.resources(Arrays.asList("*")).build());

		// AWS SSM velho api key haku
		velhoCollectorLambda
		.addToRolePolicy(PolicyStatement.Builder.create()
				.effect(Effect.ALLOW)
				.actions(Arrays.asList("secretsmanager:*"))
				.resources(Arrays.asList("arn:aws:secretsmanager:eu-central-1:426182641979:secret:VelhoSecrets-pWNDv4")).build());

		// viestinvalitys
		final Queue queue = Queue.Builder.create(this, "VelhoAnalyticsQueue").visibilityTimeout(Duration.seconds(300))
				.build();

		final Topic topic = Topic.Builder.create(this, "VelhoAnalyticsTopic").displayName("DQL for Stepfunctions alert")
				.build();

		
		/**
		 * Here we create notification to trigger lambda when velho data file is added to landing
		 * bucket
		 */
		NotificationKeyFilter filter4data = NotificationKeyFilter.builder().prefix("data/").build();
		landingBucket.addEventNotification(software.amazon.awscdk.services.s3.EventType.OBJECT_CREATED_PUT,
				new LambdaDestination(velhoDataLoderLambda), filter4data);
		
		
		
		// and a trigger for metadata files
		NotificationKeyFilter filter4metadata = NotificationKeyFilter.builder().prefix("metadata/").build();
		landingBucket.addEventNotification(software.amazon.awscdk.services.s3.EventType.OBJECT_CREATED_PUT,
				new LambdaDestination(convertMetadataLambda), filter4metadata);

		// ajasta datan lataus joka paiva 06:15
		Rule dailyRule = Rule.Builder.create(this, "DataSchedule").enabled(true)
		.description("Velho data load schedule")
		.schedule(Schedule.expression("cron(15 4 * * ? *)"))
		.build();
		
		dailyRule.addTarget(LambdaFunction.Builder.create(velhoCollectorLambda).build());
		
		// ajasta metadatan lataus joka paiva 06:05
		Rule dailyRule2 = Rule.Builder.create(this, "MetaDataSchedule").enabled(true)
		.description("Velho metadata load schedule")
		.schedule(Schedule.expression("cron(05 4 * * ? *)"))
		.build();
		
		dailyRule2.addTarget(LambdaFunction.Builder.create(getMetadataLambda).build());
		
		
		// topic.addSubscription(new EmailSubscription("")); /** */

	}

}
