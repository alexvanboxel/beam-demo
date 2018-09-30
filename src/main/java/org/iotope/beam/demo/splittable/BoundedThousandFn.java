package org.iotope.beam.demo.splittable;

import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;

import java.util.logging.Logger;

/**
 * IO to read and write data on MongoDB GridFS.
 */
@DoFn.BoundedPerElement
public class BoundedThousandFn extends DoFn<String, Long> {

    private static Logger LOG = Logger.getLogger(BoundedThousandFn.class.toString());

    @ProcessElement
    public ProcessContinuation process(ProcessContext c, OffsetRangeTracker tracker) {
        OffsetRange currentRestriction = tracker.currentRestriction();
        LOG.info("Current Restriction is "+currentRestriction.toString());

        for (long pos = tracker.currentRestriction().getFrom(); tracker.tryClaim(pos); pos++) {
            Util.claimed(LOG, currentRestriction, pos);
            c.output(pos);
        }
        return Util.stop(LOG);
    }


    @GetInitialRestriction
    public OffsetRange getInitialRestriction(String in) {
        return new OffsetRange(0, 1000);
    }

    @SplitRestriction
    public void splitRestriction(String element, OffsetRange restriction, DoFn.OutputReceiver<OffsetRange> ranges) {

        long x1 = restriction.getFrom();
        long x2 = restriction.getTo();


        ranges.output(new OffsetRange(x1, x2 / 2));
        ranges.output(new OffsetRange(x2 / 2, x2));

        System.out.println();
//        return null;
    }
}
