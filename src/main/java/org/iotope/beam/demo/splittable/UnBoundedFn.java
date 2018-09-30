package org.iotope.beam.demo.splittable;

import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Example splittable splittable that ticks every second.
 */
@DoFn.UnboundedPerElement
public class UnBoundedFn extends DoFn<String, Long> {

    private static Logger LOG = Logger.getLogger(UnBoundedFn.class.toString());


    @ProcessElement
    public ProcessContinuation process(ProcessContext c, OffsetRangeTracker tracker) {
        OffsetRange currentRestriction = tracker.currentRestriction();
        LOG.info("Current Restriction is " + currentRestriction.toString());

        long i = tracker.currentRestriction().getFrom();

        if (tracker.tryClaim(i)) {
            Util.claimed(LOG, currentRestriction, i);
            LOG.log(Level.INFO, "current pos {0}", i);
            c.output(i);
            return Util.resume(LOG, 100, "Delaying for next item");
        } else {
            return Util.stop(LOG);
        }
    }


    @GetInitialRestriction
    public OffsetRange getInitialRestriction(String in) {
        return new OffsetRange(0, Long.MAX_VALUE);
    }
}
