package org.iotope.beam.demo.splittable;

import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Duration;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Util {

    static DoFn.ProcessContinuation resume(Logger LOG, long delay, String reason) {
        LOG.log(Level.INFO, "DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis({0})) with reason [" + reason + "]", delay)
        ;
        return DoFn.ProcessContinuation.resume().withResumeDelay(Duration.millis(delay));
    }

    static DoFn.ProcessContinuation stop(Logger LOG) {
        LOG.log(Level.INFO, "DoFn.ProcessContinuation.stop()");
        return DoFn.ProcessContinuation.stop();
    }

    public static void claimed(Logger LOG, long i) {
        LOG.log(Level.INFO, "Claimed {0}", i);
    }
}
