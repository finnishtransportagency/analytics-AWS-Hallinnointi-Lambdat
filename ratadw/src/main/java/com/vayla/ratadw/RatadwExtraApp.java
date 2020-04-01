package com.vayla.ratadw;

import software.amazon.awscdk.core.App;

public class RatadwExtraApp {
    public static void main(final String[] args) {
        App app = new App();

        new RatadwExtraStack(app, "RatadwStack");

        app.synth();
    }
}
