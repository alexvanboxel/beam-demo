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
public class PerSecondTickSplitFn extends DoFn<String, Long> {

    private static Logger LOG = Logger.getLogger(PerSecondTickSplitFn.class.toString());

    @ProcessElement
    public ProcessContinuation process(ProcessContext c, OffsetRangeTracker tracker) {
        LOG.log(Level.FINER, tracker.currentRestriction().toString());

        long current = System.currentTimeMillis();
        long i = tracker.currentRestriction().getFrom();

        if (i <= current / 1000) {

            if (tracker.tryClaim(i)) {
                LOG.log(Level.INFO, "current second {0}", i);
                c.output(i);
                return Util.resume(LOG, 100, "Delaying for next item");
            } else {
                return Util.stop(LOG);
            }
        }
        return Util.resume(LOG, 750, "To early, delay");
    }


    @GetInitialRestriction
    public OffsetRange getInitialRestriction(String in) {
        return new OffsetRange(System.currentTimeMillis() / 1000, Long.MAX_VALUE);
    }
}
