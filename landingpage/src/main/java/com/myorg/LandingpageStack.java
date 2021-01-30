package com.myorg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.BucketProps;
import software.amazon.awscdk.services.s3.deployment.BucketDeployment;
import software.amazon.awscdk.services.s3.deployment.BucketDeploymentProps;
import software.amazon.awscdk.services.s3.deployment.ISource;
import software.amazon.awscdk.services.s3.deployment.Source;
import software.amazon.awscdk.services.cloudfront.CloudFrontWebDistribution;
import software.amazon.awscdk.services.cloudfront.CloudFrontWebDistributionProps;
import software.amazon.awscdk.services.cloudfront.S3OriginConfig;
import software.amazon.awscdk.services.cloudfront.SourceConfiguration;

public class LandingpageStack extends Stack {
    public LandingpageStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public LandingpageStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        // S3 bucket to hold website files
        BucketProps bucketprops = BucketProps.builder()
            .encryption(BucketEncryption.S3_MANAGED)
            .versioned(false)
            .build();

        // Let's do this!
        Bucket webbucket = new Bucket(this, "vayla-analytics-website", bucketprops);

        // CloudFront distribution for the files, with access origin identity
        S3OriginConfig s3OriginSource = S3OriginConfig.builder()
            .s3BucketSource(webbucket)
            .build();
        SourceConfiguration sourceConfiguration = SourceConfiguration.builder()
            .s3OriginSource(s3OriginSource)
            .build();
        CloudFrontWebDistributionProps webDistributionProps = CloudFrontWebDistributionProps.builder()
            .originConfigs(Arrays.asList(sourceConfiguration))
            .build();
        
        // Here we go!
        CloudFrontWebDistribution webDistribution = new CloudFrontWebDistribution(this, "vayla-website-cloudfront-dist", webDistributionProps);

        // S3 deploy to upload content from disk and distribute to cloudfront
        List<ISource> sources = new ArrayList<ISource>();
        sources.add(Source.asset("../../../webapp"));

        BucketDeploymentProps deploymentProps = BucketDeploymentProps.builder().destinationBucket(webbucket)
            .sources(sources)
            .distribution(webDistribution)
            .build();

        // Just do it!
        BucketDeployment bucketDeployment = new BucketDeployment(this, "vayla-website-deploymet", deploymentProps);

        
    }
}
