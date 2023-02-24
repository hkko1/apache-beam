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


    }

    public static final Schema SCHEMA =
            Schema.of(
                    Schema.Field.of("id", Schema.FieldType.INT32),
                    Schema.Field.of("text", Schema.FieldType.STRING),
                    Schema.Field.of("key", Schema.FieldType.STRING),
                    Schema.Field.of("transformation", Schema.FieldType.STRING),
                    Schema.Field.of("isTransformed", Schema.FieldType.BOOLEAN));


    private static PDone transformationPipeline(
            String transformationsName, TextCryptoTransformOptions options, PCollection<Row> input){

        PDone pd = input
                .apply("Encrypt or Decrypt input data",
            TextCryptoApplier.of(transformationsName, options))
                .apply("output file generate", OutputFileGenerator.of(transformationsName, options.getOutput()));

        return pd;
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