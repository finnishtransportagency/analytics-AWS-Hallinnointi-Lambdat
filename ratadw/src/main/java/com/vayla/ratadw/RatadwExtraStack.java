package com.vayla.ratadw;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.Duration;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;
import software.amazon.awscdk.services.iam.Effect;
import software.amazon.awscdk.services.iam.PolicyStatement;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.BucketProps;
import software.amazon.awscdk.services.s3.NotificationKeyFilter;
import software.amazon.awscdk.services.s3.notifications.LambdaDestination;

public class RatadwExtraStack extends Stack {
    public RatadwExtraStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public RatadwExtraStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        // The code that defines your stack goes here
        // S3 oletusominaisuudet
        final BucketProps s3BucketProps = BucketProps.builder().encryption(BucketEncryption.S3_MANAGED)
        		.versioned(true)
                .build();
        
        // Yleinen s3 ampari valiaikaisille tiedostoille ym
        Bucket workBucket = new Bucket(this, "ratadw-syykodit", s3BucketProps);


        
        /************************** CAUSE CODES ***************************/
        // Causecodes lambdan ymparistomuuttujat
 		Map<String, String> environment = new HashMap<String, String>();
 		environment.put("digitrafficHost", "rata.digitraffic.fi");
 		environment.put("causecodesURL", "/api/v1/metadata/cause-category-codes");
 		environment.put("prefix", "ratadw_syyluokat");
 		environment.put("workBucket", workBucket.getBucketName());
 		
 		// Causecodes lambda
 		final Function syyluokatLambda = Function.Builder.create(this, "VaylaRataDWSyyluokatLambda")
            .functionName("VaylaRataDWSyyluokat").timeout(Duration.minutes(5)).memorySize(1024)
            .code(Code.fromAsset("lambda" + File.separator + "causecodes" + File.separator + "ratadw-causecodes.zip"))
            .runtime(software.amazon.awscdk.services.lambda.Runtime.NODEJS_12_X).environment(environment)
            .handler("index.handler").build();
 		
 		// S3 oikeudet
 		syyluokatLambda
            .addToRolePolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW).actions(Arrays.asList("s3:*"))
                .resources(Arrays.asList("*")).build());



        /************************** DETAILED CODES ***************************/
        // Detailedcodes lambdan ymparistomuuttujat
 		Map<String, String> environment2 = new HashMap<String, String>();
 		environment2.put("digitrafficHost", "rata.digitraffic.fi");
 		environment2.put("detailedcodesURL", "/api/v1/metadata/detailed-cause-category-codes");
 		environment2.put("prefix", "ratadw_syykoodit");
 		environment2.put("workBucket", workBucket.getBucketName());
 		
 		// Detailedcodes lambda
 		final Function syykooditLambda = Function.Builder.create(this, "VaylaRataDWSyykooditLambda")
            .functionName("VaylaRataDWSyykoodit").timeout(Duration.minutes(5)).memorySize(1024)
            .code(Code.fromAsset("lambda" + File.separator + "detailedcodes" + File.separator + "ratadw-detailedcodes.zip"))
            .runtime(software.amazon.awscdk.services.lambda.Runtime.NODEJS_12_X).environment(environment2)
            .handler("index.handler").build();
 		
 		// S3 oikeudet
 		syykooditLambda
            .addToRolePolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW).actions(Arrays.asList("s3:*"))
                .resources(Arrays.asList("*")).build());



        /************************** THIRD CATEGORY CODES ***************************/
        // Thirdcategorycodes lambdan ymparistomuuttujat
 		Map<String, String> environment3 = new HashMap<String, String>();
 		environment3.put("digitrafficHost", "rata.digitraffic.fi");
 		environment3.put("thirdcategorycodesURL", "/api/v1/metadata/third-cause-category-codes");
 		environment3.put("prefix", "ratadw_tarkatsyykoodit");
 		environment3.put("workBucket", workBucket.getBucketName());
 		
 		// Detailedcodes lambda
 		final Function tarkatsyykooditLambda = Function.Builder.create(this, "VaylaRataDWTarkatSyykooditLambda")
            .functionName("VaylaRataDWTarkatSyykoodit").timeout(Duration.minutes(5)).memorySize(1024)
            .code(Code.fromAsset("lambda" + File.separator + "thirdcategorycodes" + File.separator + "ratadw-thirdcategorycodes.zip"))
            .runtime(software.amazon.awscdk.services.lambda.Runtime.NODEJS_12_X).environment(environment3)
            .handler("index.handler").build();
 		
 		// S3 oikeudet
 		tarkatsyykooditLambda
            .addToRolePolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW).actions(Arrays.asList("s3:*"))
                .resources(Arrays.asList("*")).build());

        /************************** COMBINE CODES ***************************/
        // Combinecodes lambdan ymparistomuuttujat
 		Map<String, String> environment4 = new HashMap<String, String>();
 		environment4.put("thirdcategorycodesKey", "ratadw_tarkatsyykoodit/ratadw_tarkatsyykoodit.json");
        environment4.put("detailedcodesKey", "ratadw_syykoodit/ratadw_syykoodit.json");
        environment4.put("causecodesKey", "ratadw_syyluokat/ratadw_syyluokat.json");
        environment4.put("csvprefix", "ratadw_koodisto/ratadw_koodisto.csv");
 		environment4.put("workBucket", workBucket.getBucketName());
 		
 		// Combinecodes lambda
 		final Function yhdistekooditLambda = Function.Builder.create(this, "VaylaRataDWSyykoodiYhdistajaLambda")
            .functionName("VaylaRataDWSyykoodiYhdistaja").timeout(Duration.minutes(5)).memorySize(1024)
            .code(Code.fromAsset("lambda" + File.separator + "combinedcodes" + File.separator + "ratadw-combinedcodes.zip"))
            .runtime(software.amazon.awscdk.services.lambda.Runtime.NODEJS_12_X).environment(environment4)
            .handler("index.handler").build();
 		
 		// S3 oikeudet
 		yhdistekooditLambda
            .addToRolePolicy(PolicyStatement.Builder.create()
                .effect(Effect.ALLOW).actions(Arrays.asList("s3:*"))
                .resources(Arrays.asList("*")).build());

        /************************** CSV TO ADE ***************************/
        // Manifest lambdan ymparistomuuttujat
        Map<String, String> environment5 = new HashMap<String, String>();
 		environment5.put("workBucket", workBucket.getBucketName());
 		environment5.put("adeBucket", "dummyadebucket");
 		
 		// Manifest Lambda
 		final Function csv2adeLambda = Function.Builder.create(this, "RataDWCsv2AdeLambda")
				.functionName("RataDWCsv2Ade").timeout(Duration.minutes(5)).memorySize(1024)
				.code(Code.fromAsset("lambda" + File.separator + "csv2ade" + File.separator + "ratadw-csv2ade.zip"))
				.runtime(software.amazon.awscdk.services.lambda.Runtime.NODEJS_12_X).environment(environment5)
				.handler("index.handler").build();
 		
 		// S3 oikeudet
 		csv2adeLambda
		.addToRolePolicy(PolicyStatement.Builder.create()
				.effect(Effect.ALLOW).actions(Arrays.asList("s3:*"))
				.resources(Arrays.asList("*")).build());
 		
 		// Triggeri csv tiedostoille, joka kaynnistaa manifest luonnin
 		// ja aden kansioon kopioinnin
 		NotificationKeyFilter csvfilter = NotificationKeyFilter.builder().suffix(".csv").build();
 		workBucket.addEventNotification(software.amazon.awscdk.services.s3.EventType.OBJECT_CREATED_PUT, 
 				new LambdaDestination(csv2adeLambda), csvfilter);
    }
}
