package org.apache.beam.sdk.io.mongodb;

import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;

/**
 * IO to read and write data on MongoDB GridFS.
 */
@DoFn.UnboundedPerElement
public class Tick1StreamFn extends DoFn<String, String> {


    @ProcessElement
    public ProcessContinuation process(ProcessContext c, OffsetRangeTracker tracker) {
        System.out.println(tracker.currentRestriction().toString());

        long current = System.currentTimeMillis();
        if(tracker.tryClaim(current/1000)) {
            long min = tracker.currentRestriction().getFrom();
            for(long i=current/1000;i<min;i--) {
                System.out.println("i"+i);
                c.output("i"+i);
            }
            System.out.println("resume");
//            return ProcessContinuation.resume().withResumeDelay(Duration.millis(1000-current%1000));
            return ProcessContinuation.resume();

        }
        System.out.println("stop");
        return ProcessContinuation.stop();

    }


    @GetInitialRestriction
    public OffsetRange getInitialRestriction(String in) {
        return new OffsetRange(System.currentTimeMillis()/1000, Long.MAX_VALUE);
    }

//    @SplitRestriction
//    public void splitRestriction(String element, OffsetRange restriction, DoFn.OutputReceiver<OffsetRange> ranges) {
//
//        long x1 = restriction.getFrom();
//        long x2 = restriction.getTo();
//
//
////
////        ranges.output(new OffsetRange(0, 500));
////        ranges.output(new OffsetRange(500, 1000));
//
//        System.out.println();
////        return null;
//    }
}
