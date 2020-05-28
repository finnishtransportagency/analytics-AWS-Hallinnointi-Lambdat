package com.vayla;

import software.amazon.awscdk.core.App;

public final class VelhoAnalyticsApp {
    public static void main(final String[] args) {
        App app = new App();

        new VelhoAnalyticsStack(app, "VelhoAnalyticsStack");

        app.synth();
    }
}
