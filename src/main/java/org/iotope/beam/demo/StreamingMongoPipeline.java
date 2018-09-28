package org.iotope.beam.demo;

import org.apache.beam.sdk.Pipeline;
import org.iotope.beam.demo.splittable.SecondTickSplitFn;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

public class StreamingMongoPipeline extends DemoPipelineBase {


    public static void main(String... args) {
        new StreamingMongoPipeline().run();
    }

    public void run() {


        String URL = env(MONGO_URL);


        Pipeline pipeline = createPipeline();

//        PCollection<Document> in = pipeline.apply(
//                MongoDbIO.read().withUri(URL)
//                        .withDatabase("staging-carts")
//                        .withCollection("carts")
//        );


        pipeline.apply(Create.of("")).apply(
                ParDo.of(new SecondTickSplitFn(60))
        )
                .apply(ParDo.of(new DoFn<Long, String>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        c.output(c.element().toString());
                    }
                }))
                .apply(ParDo.of(stringToTableRowFn()))
                .apply(
                        bigqueryIO()
                )
        ;
        pipeline.run();

    }

//TODO what about .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
}
