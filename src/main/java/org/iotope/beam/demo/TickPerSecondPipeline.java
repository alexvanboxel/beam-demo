package org.iotope.beam.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.iotope.beam.demo.splittable.SecondTickSplitFn;

public class TickPerSecondPipeline extends DemoPipelineBase {


    public static void main(String... args) {
        new TickPerSecondPipeline().run();
    }

    public void run() {

        Pipeline pipeline = createPipeline();
        pipeline.apply(Create.of("")).apply(
                ParDo.of(new SecondTickSplitFn(5))
        )
                .apply(ParDo.of(longToStringFn()))
                .apply(ParDo.of(stringToTableRowFn()))
        ;

        pipeline.run();

    }

}
