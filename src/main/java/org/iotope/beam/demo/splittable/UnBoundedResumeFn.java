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
public class UnBoundedResumeFn extends DoFn<String, Long> {

    private static Logger LOG = Logger.getLogger(UnBoundedResumeFn.class.toString());


    @ProcessElement
    public ProcessContinuation process(ProcessContext c, OffsetRangeTracker tracker) {
        OffsetRange currentRestriction = tracker.currentRestriction();
        LOG.info("Current Restriction is " + currentRestriction.toString());

        if (currentRestriction.getTo() == Long.MAX_VALUE) {
            long i = currentRestriction.getFrom();

            if (tracker.tryClaim(i)) {
                Util.claimed(LOG, currentRestriction, i);
                LOG.log(Level.INFO, "current pos {0}", i);
                Util.output(LOG, currentRestriction, i, c, i);
                return Util.resume(LOG, 100, "Delaying for next item");
            }
        } else {
            for (long pos = tracker.currentRestriction().getFrom(); tracker.tryClaim(pos); pos++) {
                Util.claimed(LOG, currentRestriction, pos);
                Util.output(LOG, currentRestriction, pos, c, pos);
                c.output(pos);
            }
        }
        return Util.stop(LOG);
    }


    @GetInitialRestriction
    public OffsetRange getInitialRestriction(String in) {
        return new OffsetRange(0, Long.MAX_VALUE);
    }

//    @SplitRestriction
//    public void splitRestriction(String element, OffsetRange restriction, DoFn.OutputReceiver<OffsetRange> ranges) {
//
//        long x1 = restriction.getFrom();
//        long x2 = restriction.getTo();
//
//
//        ranges.output(new OffsetRange(0, 1000));
//        ranges.output(new OffsetRange(1000, Long.MAX_VALUE));
//
//        System.out.println();
////        return null;
//    }

}
