package org.apache.beam.examples.cryptograph;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

public class OutputFileGenerator extends PTransform<PCollection<Row>, PDone> {

    String transformation;
    String outputName;

    private OutputFileGenerator(String transformation, String outputName){
        this.transformation = transformation;
        this.outputName = outputName;
    }
    public static OutputFileGenerator of(String transformation, String outputName){
        return new OutputFileGenerator(transformation, outputName);
    }
    static class FormatAsCsvFn extends SimpleFunction<Row, String> {
        @Override
        public String apply(Row input) {
            String result = input.getInt32("id") + ","
                    + input.getString("text") + ","
                    + input.getString("key") + ","
                    + input.getString("transformation");
            return result;
        }
    }

    static class FormatAsTextFn extends SimpleFunction<KV<Boolean, Long>, String>{
        @Override
        public String apply(KV<Boolean, Long> input) {
            String result = input.getKey() ? "Success" : "Failure";
            return result + ": " + input.getValue();
        }
    }

    static class DoExtractTransformationResultFn extends DoFn<Row, Boolean> {
        @ProcessElement
        public void processExtractTransformationResult(@Element Row element, OutputReceiver<Boolean> receiver){
            Boolean isTransformed = element.getBoolean("isTransformed");
            receiver.output(isTransformed);
        }
    }
    static class CountTransformedData extends PTransform<PCollection<Row>, PCollection<KV<Boolean, Long>>> {

        @Override
        public PCollection<KV<Boolean, Long>> expand(PCollection<Row> input) {
            PCollection<Boolean> transformedResult = input.apply(ParDo.of(new DoExtractTransformationResultFn()));

            // Count the number of times each word occurs.
            PCollection<KV<Boolean, Long>> counts = transformedResult.apply(Count.perElement());

            return counts;
        }
    }
    @Override
    public PDone expand(PCollection<Row> input) {
        Pipeline p = input.getPipeline();
        Schema textInputSchema = input.getSchema();
        System.out.println("[OutputFileGenerator]-transformation:"+transformation);
        input.apply("TextData to String(for csv)", MapElements.via(new FormatAsCsvFn()))
                //.apply("WriteEncryptText", TextIO.write().to(options.getOutput()).withoutSharding().withSuffix(".csv"));
                .apply("WriteEncryptText", TextIO.write().to(outputName).withoutSharding().withSuffix(".csv"));
        //.apply("WriteEncryptText", TextIO.write().to(options.getOutput()));

        //Output: .txt file which has the number of transformed data
        input.apply("Count the number of transformed data", new CountTransformedData())
                .apply("BooleanResult to String(for txt)", MapElements.via(new FormatAsTextFn()))
                //.apply("WriteCountResult", TextIO.write().to(options.getOutput()).withoutSharding().withSuffix(".txt"));
                .apply("WriteCountResult", TextIO.write().to(outputName).withoutSharding().withSuffix(".txt"));

        return PDone.in(p);
    }
}
