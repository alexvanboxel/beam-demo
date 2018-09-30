package org.iotope.beam.demo.splittable;

import org.apache.beam.sdk.io.range.OffsetRange;
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

    /**
     * Log the current claim, no side effects.
     */
    public static void claimed(Logger LOG, OffsetRange currentRestriction, long i) {
        LOG.log(Level.INFO, "Claimed [{0}] on range " + currentRestriction.toString(), i);
    }

    public static void output(Logger LOG, OffsetRange currentRestriction, long claimPos, DoFn<String, Long>.ProcessContext c, long i) {
        LOG.log(Level.INFO, "Output  [{0}] on range " + currentRestriction.toString() + " claim position: " + claimPos, i);
        c.output(i);
    }
}
