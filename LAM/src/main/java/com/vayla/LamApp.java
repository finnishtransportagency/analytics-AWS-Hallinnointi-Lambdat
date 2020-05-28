package com.vayla;

import software.amazon.awscdk.core.App;


import java.util.Arrays;

public class LamApp {
    public static void main(final String[] args) {
        App app = new App();
        new LamStack(app, "LamStack");

        app.synth();
    }
}
