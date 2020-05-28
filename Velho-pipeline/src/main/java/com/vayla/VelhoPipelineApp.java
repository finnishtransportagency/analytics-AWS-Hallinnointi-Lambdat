package com.vayla;

import software.amazon.awscdk.core.App;

import java.util.Arrays;

public class VelhoPipelineApp {
    public static void main(final String[] args) {
        App app = new App();

        new VelhoPipelineStack(app, "VelhoPipelineStack");

        app.synth();
    }
}
