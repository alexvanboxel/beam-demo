package org.iotope.beam.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.iotope.beam.demo.splittable.BoundedOverlapFn;
import org.iotope.beam.demo.splittable.BoundedThousandFn;
import org.iotope.beam.demo.splittable.UnBoundedFn;

public class ScrapbookPipeline extends DemoPipelineBase {


    public static void main(String... args) {
        new ScrapbookPipeline().run();
    }

    public void run() {

        Pipeline pipeline = createPipeline();
        pipeline.apply(Create.of("")).apply(
                ParDo.of(new BoundedOverlapFn())
        )
                .apply(ParDo.of(longToStringFn()))
                .apply(ParDo.of(stringToTableRowFn()))
        ;

        pipeline.run();

    }

}
