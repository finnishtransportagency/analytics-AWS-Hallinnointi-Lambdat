package com.myorg;

import software.amazon.awscdk.core.App;

import java.util.Arrays;

public class LandingpageApp {
    public static void main(final String[] args) {
        App app = new App();

        new LandingpageStack(app, "LandingpageStack");

        app.synth();
    }
}
