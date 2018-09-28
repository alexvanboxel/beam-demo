package org.iotope.beam.demo.splittable;

import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.joda.time.Duration;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Example splittable splittable that ticks every second.
 */
@DoFn.UnboundedPerElement
public class SecondTickSplitFn extends DoFn<String, Long> {

    private static Logger LOG = Logger.getLogger(SecondTickSplitFn.class.toString());

    private long seconds;

    public SecondTickSplitFn(long seconds) {
        this.seconds = seconds;
    }

    public long delay(long item) {
        long ims = item * (seconds * 1000);
        long curr = System.currentTimeMillis();
        long wait = ims - curr;
        if (wait < 0) {
            return 10;
        }
        return wait;
    }

    @ProcessElement
    public ProcessContinuation process(ProcessContext c, OffsetRangeTracker tracker) {
        LOG.log(Level.INFO, tracker.currentRestriction().toString());

        long current = System.currentTimeMillis();
        long i = tracker.currentRestriction().getFrom();

        if (i * (seconds * 1000) <= current) {
            if (tracker.tryClaim(i)) {
                Util.claimed(LOG, i);
                LOG.log(Level.INFO, "current second {0}", i * seconds);
                c.output(i * seconds);
                return Util.resume(LOG, delay(i + 1), "Delaying for next item");
            } else {
                return Util.stop(LOG);
            }
        }
        return Util.resume(LOG, delay(i), "To early, delay");
    }


    @GetInitialRestriction
    public OffsetRange getInitialRestriction(String in) {
        return new OffsetRange(System.currentTimeMillis() / (seconds * 1000), Long.MAX_VALUE);
    }
}
