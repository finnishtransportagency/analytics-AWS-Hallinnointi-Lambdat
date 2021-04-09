package com.myorg;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;
import software.amazon.awscdk.services.certificatemanager.Certificate;
import software.amazon.awscdk.services.certificatemanager.ICertificate;
import software.amazon.awscdk.services.cloudfront.AliasConfiguration;
import software.amazon.awscdk.services.cloudfront.Behavior;
import software.amazon.awscdk.services.cloudfront.CloudFrontWebDistribution;
import software.amazon.awscdk.services.cloudfront.CloudFrontWebDistributionProps;
import software.amazon.awscdk.services.cloudfront.HttpVersion;
import software.amazon.awscdk.services.cloudfront.OriginAccessIdentity;
import software.amazon.awscdk.services.cloudfront.OriginAccessIdentityProps;
import software.amazon.awscdk.services.cloudfront.S3OriginConfig;
import software.amazon.awscdk.services.cloudfront.SourceConfiguration;
import software.amazon.awscdk.services.cloudfront.ViewerCertificate;
import software.amazon.awscdk.services.cloudfront.ViewerCertificateOptions;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.BucketProps;
import software.amazon.awscdk.services.s3.deployment.BucketDeployment;
import software.amazon.awscdk.services.s3.deployment.BucketDeploymentProps;
import software.amazon.awscdk.services.s3.deployment.ISource;
import software.amazon.awscdk.services.s3.deployment.Source;

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
            .isDefaultBehavior(true)
            .build(); 

        SourceConfiguration s3SourceConfiguration = SourceConfiguration.builder()
            .s3OriginSource(s3OriginSource)
            .behaviors(Arrays.asList(s3behavior))
            .build();

        ICertificate certificate = Certificate.fromCertificateArn(this, "data-vayla-fi-cert", "arn:aws:acm:us-east-1:169978597495:certificate/382e9e98-2158-4f3e-b871-1b18976d5286");
        ViewerCertificateOptions coptions = ViewerCertificateOptions.builder()
            .aliases(Arrays.asList("palvelut.data.vayla.fi"))
            .build();

        ViewerCertificate viewerCertificate = ViewerCertificate
            .fromAcmCertificate(certificate, coptions);

        HttpVersion httpVersion = HttpVersion.HTTP2;

        CloudFrontWebDistributionProps webDistributionProps = CloudFrontWebDistributionProps.builder()
            .originConfigs(Arrays.asList(s3SourceConfiguration))
            .viewerCertificate(viewerCertificate)
            .defaultRootObject("index.html")
            .enableIpV6(true)
            .httpVersion(httpVersion)
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
