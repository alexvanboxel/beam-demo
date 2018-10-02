package org.iotope.beam.demo.splittable;

import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;

import java.util.logging.Logger;

/**
 * IO to read and write data on MongoDB GridFS.
 */
@DoFn.UnboundedPerElement
public class UnboundedOverlapFn extends DoFn<String, Long> {

    private static Logger LOG = Logger.getLogger(UnboundedOverlapFn.class.toString());

    private long nextValidPosition(long pos, long mod) {
        if (pos % mod == 0) {
            return pos;
        }
        return pos + (mod - (pos % mod));
    }

    @ProcessElement
    public void process(ProcessContext c, OffsetRangeTracker tracker) {
        OffsetRange currentRestriction = tracker.currentRestriction();
        LOG.info("Current Restriction is " + currentRestriction.toString());

        long pos = currentRestriction.getFrom();
        int mod = 7;
        pos = nextValidPosition(pos, mod);

        for (; tracker.tryClaim(pos); ) {
            long claimPos = pos;
            Util.claimed(LOG, currentRestriction, pos);
            for (int o = 0; o < mod; o++) {
                Util.output(LOG, currentRestriction, claimPos, c, pos++);
            }
        }
        Util.stop(LOG);
    }


    @GetInitialRestriction
    public OffsetRange getInitialRestriction(String in) {
        return new OffsetRange(0, Long.MAX_VALUE);
    }

//    @SplitRestriction
//    public void splitRestriction(String element, OffsetRange restriction, OutputReceiver<OffsetRange> ranges) {
//        long x2 = restriction.getTo();
//        ranges.output(new OffsetRange(restriction.getFrom(), x2 / 2));
//        ranges.output(new OffsetRange(x2 / 2, x2));
//        System.out.println();
//    }
}
