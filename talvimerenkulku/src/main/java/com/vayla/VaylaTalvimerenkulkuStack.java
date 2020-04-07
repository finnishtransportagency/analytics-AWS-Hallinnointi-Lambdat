package com.vayla;

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

public class VaylaTalvimerenkulkuStack extends Stack {
    public VaylaTalvimerenkulkuStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public VaylaTalvimerenkulkuStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        // S3 oletusominaisuudet
        final BucketProps s3BucketProps = BucketProps.builder().encryption(BucketEncryption.S3_MANAGED)
        		.versioned(true)
				.build();

        Bucket workBucket = new Bucket(this, "fmi-saatiedot", s3BucketProps);
        
        /****************************************************************************************************/
        // Havainto Lambdan ymparistomuuttujat
 		Map<String, String> environment = new HashMap<String, String>();
 		environment.put("fmiHost", "data.fmi.fi");
 		environment.put("apiKey", "xxxxx");
 		environment.put("havainnotURL", "/wfs?request=getfeature&storedquery_id=fmi::observations::weather::daily::timevaluepair&fmisid=101237&parameters=tday");
 		environment.put("prefix", "fmi_havainto");
 		environment.put("workBucket", workBucket.getBucketName());
 		
 		// havaintodata Lambda
 		final Function havaintoLambda = Function.Builder.create(this, "VaylaFMIHavaintodataLambda")
				.functionName("VaylaFMIHavaintodata").timeout(Duration.minutes(5)).memorySize(1024)
				.code(Code.fromAsset("lambda" + File.separator + "fmihavaintodata" + File.separator + "fmi-havaintodata.zip"))
				.runtime(software.amazon.awscdk.services.lambda.Runtime.NODEJS_12_X).environment(environment)
				.handler("index.handler").build();
 		
 		// S3 oikeudet
 		havaintoLambda
		.addToRolePolicy(PolicyStatement.Builder.create()
				.effect(Effect.ALLOW).actions(Arrays.asList("s3:*"))
				.resources(Arrays.asList("*")).build());
 		
 		/****************************************************************************************************/
 		// Havaintoasema lambdan ymparistomuuttujat
 		environment = new HashMap<String, String>();
 		environment.put("fmiHost", "data.fmi.fi");
 		environment.put("apiKey", "xxxxx");
 		environment.put("havaintoasemaURL", "/wfs?service=WFS&version=2.0.0&request=getFeature&storedquery_id=fmi::ef::stations&fmisid=101237");
 		environment.put("prefix", "fmi_asemat");
 		environment.put("workBucket", workBucket.getBucketName());
 		
 		// havaintodata Lambda
 		final Function asemaLambda = Function.Builder.create(this, "VaylaFMIHavaintoasematLambda")
				.functionName("VaylaFMIHavaintoasemat").timeout(Duration.minutes(5)).memorySize(1024)
				.code(Code.fromAsset("lambda" + File.separator + "fmihavaintoasemat" + File.separator + "fmi-havaintoasemat.zip"))
				.runtime(software.amazon.awscdk.services.lambda.Runtime.NODEJS_12_X).environment(environment)
				.handler("index.handler").build();
 		
 		// S3 oikeudet
 		asemaLambda
		.addToRolePolicy(PolicyStatement.Builder.create()
				.effect(Effect.ALLOW).actions(Arrays.asList("s3:*"))
				.resources(Arrays.asList("*")).build());
 		
 		/****************************************************************************************************/
 		// Ennuste lambdan ymparistomuuttujat
 		environment = new HashMap<String, String>();
 		environment.put("fmiHost", "data.fmi.fi");
 		environment.put("apiKey", "xxxxx");
 		environment.put("ennusteetURL", "/wfs?request=getFeature&storedquery_id=fmi::forecast::ecmwf::europe::daily00::multipointcoverage&latlons=61.040298,28.129162");
 		environment.put("prefix", "fmi_ennuste");
 		environment.put("fmisid", "101237");
 		environment.put("workBucket", workBucket.getBucketName());
 		
 		// Ennuste Lambda
 		final Function ennusteLambda = Function.Builder.create(this, "VaylaFMISaaennusteLambda")
				.functionName("VaylaFMISaaennuste").timeout(Duration.minutes(5)).memorySize(1024)
				.code(Code.fromAsset("lambda" + File.separator + "fmisaaennusteet" + File.separator + "fmi-saaennusteet.zip"))
				.runtime(software.amazon.awscdk.services.lambda.Runtime.NODEJS_12_X).environment(environment)
				.handler("index.handler").build();
 		
 		// S3 oikeudet
 		ennusteLambda
		.addToRolePolicy(PolicyStatement.Builder.create()
				.effect(Effect.ALLOW).actions(Arrays.asList("s3:*"))
				.resources(Arrays.asList("*")).build());
 		
 		/****************************************************************************************************/
 		// Fraktiilit lambdan ymparistomuuttujat
 		environment = new HashMap<String, String>();
 		environment.put("fmiHost", "data.fmi.fi");
 		environment.put("apiKey", "xxxxx");
 		environment.put("fraktiilitURL", "/wfs?request=getFeature&storedquery_id=fmi::forecast::ecmwf::europe::surface::fractile::coastadjusted::multipointcoverage&latlons=61.040298,28.129162");
 		environment.put("prefix", "fmi_ennuste_fraktiilit");
 		environment.put("fmisid", "101237");
 		environment.put("workBucket", workBucket.getBucketName());
 		
 		// Fraktiilit Lambda
 		final Function fraktiiliLambda = Function.Builder.create(this, "VaylaFMIFraktiiliLambda")
				.functionName("VaylaFMIFraktiilit").timeout(Duration.minutes(5)).memorySize(1024)
				.code(Code.fromAsset("lambda" + File.separator + "fmifraktiilit" + File.separator + "fmi-fraktiilit.zip"))
				.runtime(software.amazon.awscdk.services.lambda.Runtime.NODEJS_12_X).environment(environment)
				.handler("index.handler").build();
 		
 		// S3 oikeudet
 		fraktiiliLambda
		.addToRolePolicy(PolicyStatement.Builder.create()
				.effect(Effect.ALLOW).actions(Arrays.asList("s3:*"))
				.resources(Arrays.asList("*")).build());
 		
 		/****************************************************************************************************/
 		// Manifesti lambdan ymparistomuuttujat
 		environment = new HashMap<String, String>();
 		environment.put("workBucket", workBucket.getBucketName());
 		environment.put("adeBucket", "dummyadebucket");
 		
 		// Fraktiilit Lambda
 		final Function manifestLambda = Function.Builder.create(this, "VaylaFMIManifestLambda")
				.functionName("VaylaFMIManifestit").timeout(Duration.minutes(5)).memorySize(1024)
				.code(Code.fromAsset("lambda" + File.separator + "fmimanifestit" + File.separator + "fmi-manifestit.zip"))
				.runtime(software.amazon.awscdk.services.lambda.Runtime.NODEJS_12_X).environment(environment)
				.handler("index.handler").build();
 		
 		// S3 oikeudet
 		manifestLambda
		.addToRolePolicy(PolicyStatement.Builder.create()
				.effect(Effect.ALLOW).actions(Arrays.asList("s3:*"))
				.resources(Arrays.asList("*")).build());
 		
 		// Triggeri csv tiedostoille, joka kaynnistaa manifest luonnin
 		// ja aden kansioon kopioinnin
 		NotificationKeyFilter csvfilter = NotificationKeyFilter.builder().suffix(".csv").build();
 		workBucket.addEventNotification(software.amazon.awscdk.services.s3.EventType.OBJECT_CREATED_PUT, 
 				new LambdaDestination(manifestLambda), csvfilter);
 		
 		
 		// ajasta datan lataus joka paiva 04:35 tilin paikallista aikaa
		Rule dailyRule = Rule.Builder.create(this, "DataSchedule").enabled(true)
		.description("FMI data load schedule")
		.schedule(Schedule.expression("cron(35 4 * * ? *)"))
		.build();
		
		dailyRule.addTarget(LambdaFunction.Builder.create(havaintoLambda).build());
		dailyRule.addTarget(LambdaFunction.Builder.create(asemaLambda).build());
		dailyRule.addTarget(LambdaFunction.Builder.create(ennusteLambda).build());
		dailyRule.addTarget(LambdaFunction.Builder.create(fraktiiliLambda).build());
    }
}
