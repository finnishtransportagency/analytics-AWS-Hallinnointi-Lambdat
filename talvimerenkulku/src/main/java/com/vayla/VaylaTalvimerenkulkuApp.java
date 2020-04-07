package com.vayla;

import software.amazon.awscdk.core.App;

public class VaylaTalvimerenkulkuApp {
    public static void main(final String[] args) {
        App app = new App();

        new VaylaTalvimerenkulkuStack(app, "VaylaTalvimerenkulkuStack");

        app.synth();
    }
}
