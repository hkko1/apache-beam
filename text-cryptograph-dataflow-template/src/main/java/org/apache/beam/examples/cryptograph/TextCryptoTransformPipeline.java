package org.apache.beam.examples.cryptograph;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

import java.util.Map;

public class TextCryptoTransformPipeline{

    public interface TextCryptoTransformOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("input.csv")
        String getInputFile();
        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();
        void setOutput(String value);

        @Description("Choose between encryption and decryption")
        @Default.String("encryption")
        String getCryptoOperation();
        void setCryptoOperation(String value);

        @Description("Project id. Required when running a Dataflow in the cloud")
        @Default.String("TextCrypto")
        String getProject();

        void setProject(String value);

    }

    public static final Schema SCHEMA =
            Schema.of(
                    Schema.Field.of("id", Schema.FieldType.INT32),
                    Schema.Field.of("text", Schema.FieldType.STRING),
                    Schema.Field.of("key", Schema.FieldType.STRING),
                    Schema.Field.of("transformation", Schema.FieldType.STRING),
                    Schema.Field.of("isTransformed", Schema.FieldType.BOOLEAN));

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

    static class DoExtractTransformationResultFn extends DoFn<Row, Boolean>{
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
    private static PDone transformationPipeline(
            String transformationsName, TextCryptoTransformOptions options, PCollection<Row> input){

        PCollection<Row> rowCollection = input.apply("Encrypt or Decrypt input data",
            TextCryptoApplier.of(transformationsName, options));

        //Output : .csv file which has the transformed data
        rowCollection.apply("TextData to String(for csv)", MapElements.via(new FormatAsCsvFn()))
                .apply("WriteEncryptText", TextIO.write().to(options.getOutput()).withoutSharding().withSuffix(".csv"));
        //.apply("WriteEncryptText", TextIO.write().to(options.getOutput()));

        //Output: .txt file which has the number of transformed data
        rowCollection.apply("Count the number of transformed data", new CountTransformedData())
                .apply("BooleanResult to String(for txt)", MapElements.via(new FormatAsTextFn()))
                .apply("WriteCountResult", TextIO.write().to(options.getOutput()).withoutSharding().withSuffix(".txt"));


    }
    private static PipelineResult runTextCryptoTransform(TextCryptoTransformOptions options){
        Pipeline p = Pipeline.create(options);

        TextCryptoPCollectionFetchers pcollectionFetcher = TextCryptoPCollectionFetchers.create(options);

        Map<String, PCollection<Row>> pcollections = pcollectionFetcher.convertInputPcollections(p);

        for(Map.Entry<String, PCollection<Row>> textInputEntry : pcollections.entrySet()){
            String transformationName = textInputEntry.getKey();
            PCollection<Row> textInputData = textInputEntry.getValue();
            transformationPipeline(transformationName, options, textInputData);
        }

        PipelineResult result = p.run();

        return result;
    }
    public static void main(String[] args){
        TextCryptoTransformOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TextCryptoTransformOptions.class);
        System.out.println("Hello TextTransformation Dataflow Template");
        runTextCryptoTransform(options);
    }
}