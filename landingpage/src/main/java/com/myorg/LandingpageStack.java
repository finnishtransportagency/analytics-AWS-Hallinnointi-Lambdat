package com.myorg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import software.amazon.awscdk.services.certificatemanager.Certificate;
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
import software.amazon.awscdk.services.cloudfront.Behavior;
import software.amazon.awscdk.services.cloudfront.CloudFrontWebDistribution;
import software.amazon.awscdk.services.cloudfront.CloudFrontWebDistributionProps;
import software.amazon.awscdk.services.cloudfront.CustomOriginConfig;
import software.amazon.awscdk.services.cloudfront.OriginAccessIdentity;
import software.amazon.awscdk.services.cloudfront.OriginAccessIdentityProps;
import software.amazon.awscdk.services.cloudfront.OriginProtocolPolicy;
import software.amazon.awscdk.services.cloudfront.S3OriginConfig;
import software.amazon.awscdk.services.cloudfront.SourceConfiguration;
import software.amazon.awscdk.services.cloudfront.ViewerCertificate;

public class LandingpageStack extends Stack {
    public LandingpageStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public LandingpageStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        /* S3 bucket 
         * to hold website files */
        BucketProps bucketprops = BucketProps.builder()
            .encryption(BucketEncryption.S3_MANAGED)
            .versioned(false)
            .build();

        // Let's do this!
        Bucket webbucket = new Bucket(this, "vayla-analytics-website", bucketprops);

        /* CloudFront distribution
        * with separate behaviors and paths for static content from s3
        * and default behavior for all the other paths (aka dynamic tableau/cognos content through LB)
        */
        OriginAccessIdentityProps oaiProps = OriginAccessIdentityProps.builder().build();
        OriginAccessIdentity originAccessIdentity = new OriginAccessIdentity(this, "vayla-origin-access-identity", oaiProps);
        S3OriginConfig s3OriginSource = S3OriginConfig.builder()
            .s3BucketSource(webbucket)
            .originAccessIdentity(originAccessIdentity)
            //.originPath("/datahub")
            .build();

        //Path based behaviors for static content from s3
        Behavior s3behavior = Behavior.builder()
            .pathPattern("datahub/images/*")
            .build(); 
        
        Behavior s3behavior2 = Behavior.builder()
            .pathPattern("datahub/css/*")
            .build();
        
        Behavior s3behavior3 = Behavior.builder()
            .pathPattern("datahub/index.html")
            .build();

        SourceConfiguration s3SourceConfiguration = SourceConfiguration.builder()
            .s3OriginSource(s3OriginSource)
            .behaviors(Arrays.asList(s3behavior,s3behavior2,s3behavior3))
            .build();

        CustomOriginConfig customOriginSource = CustomOriginConfig.builder()
            .domainName("tableu-LB-1374501584.eu-central-1.elb.amazonaws.com")
            .httpPort(80)
            .httpsPort(443)
            .originProtocolPolicy(OriginProtocolPolicy.HTTP_ONLY) // TODO: change to match viewer (http + https) when dns etc settings allow it?
            .build();

        // TODO: disable caching for dynamic content, but cant find API for it?
        // cache settings like legacy vs. policy etc and ie. "managed-cachingdisabled" policy
        // which are available in console
        // also viewer protocol policy to "http and https"
        Behavior cognosBehavior = Behavior.builder()
            .pathPattern("ibmcognos/*")
            .build();

        // All the rest paths for Loadbalancer origin including hopefully tableau paths
        // currently not working because of #-symbol in the urls
        Behavior defaultBehavior = Behavior.builder()
            .isDefaultBehavior(true)
            .build();

        SourceConfiguration lbSourceConfiguration = SourceConfiguration.builder()
            .customOriginSource(customOriginSource)
            .behaviors(Arrays.asList(cognosBehavior, defaultBehavior))
            .build();

        ViewerCertificate viewerCertificate = ViewerCertificate
            .fromAcmCertificate(Certificate.fromCertificateArn(this, "data-vayla-fi-cert", "arn:aws:acm:us-east-1:169978597495:certificate/382e9e98-2158-4f3e-b871-1b18976d5286"));

        CloudFrontWebDistributionProps webDistributionProps = CloudFrontWebDistributionProps.builder()
            .originConfigs(Arrays.asList(s3SourceConfiguration, lbSourceConfiguration))
            .viewerCertificate(viewerCertificate)
            .defaultRootObject("/")
            .build();
        
        // Here we go!
        CloudFrontWebDistribution webDistribution = new CloudFrontWebDistribution(this, "vayla-website-cloudfront-dist", webDistributionProps);

        /* S3 deploy 
         * to upload content from disk and distribute to cloudfront */
        List<ISource> sources = new ArrayList<ISource>();
        sources.add(Source.asset("src/main/webapp"));

        BucketDeploymentProps deploymentProps = BucketDeploymentProps.builder().destinationBucket(webbucket)
            .sources(sources)
            .distribution(webDistribution)
            .build();

        // Just do it!
        BucketDeployment bucketDeployment = new BucketDeployment(this, "vayla-website-deploymet", deploymentProps);

    }
}
