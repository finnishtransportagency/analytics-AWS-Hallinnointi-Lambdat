package com.vayla;

import javax.swing.TransferHandler;

import software.amazon.awscdk.core.Construct;
import software.amazon.awscdk.core.Stack;
import software.amazon.awscdk.core.StackProps;
import software.amazon.awscdk.services.ec2.*;
import software.amazon.awscdk.services.transfer.*;
import software.amazon.awscdk.services.transfer.CfnServer.EndpointDetailsProperty;

public class LamStack extends Stack {
    public LamStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public LamStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);


        Vpc vpc = Vpc.Builder.create(this, "Lam-VPC").maxAzs(2).build();

/*
        String[] subnet;
        for (ISubnet x : vpc.getPublicSubnets()) {
            subnet

        }

        CfnServer.Builder.create(this, "lam-sftp-service")
        .endpointDetails(
            EndpointDetailsProperty.builder()
            .subnetIds(
            
            ).build();
            
            )
*/


        






        // The code that defines your stack goes here
    }
}
