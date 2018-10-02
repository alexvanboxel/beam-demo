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
public class UnBoundedLoopFn extends DoFn<String, Long> {

    private static Logger LOG = Logger.getLogger(UnBoundedLoopFn.class.toString());


    @ProcessElement
    public ProcessContinuation process(ProcessContext c, OffsetRangeTracker tracker) {
        OffsetRange currentRestriction = tracker.currentRestriction();
        LOG.info("Current Restriction is " + currentRestriction.toString());

        for (long pos = tracker.currentRestriction().getFrom(); tracker.tryClaim(pos); pos++) {
            Util.claimed(LOG, currentRestriction, pos);
            Util.output(LOG, currentRestriction, pos, c, pos);
            c.output(pos);
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
