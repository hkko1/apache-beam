package org.apache.beam.examples.cryptograph;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import javax.xml.soap.Text;
import java.util.Arrays;

public class TextEncryption {
    public interface TextEncryptionOptions extends PipelineOptions{
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

    //Schema for input data from csv file
    public static final Schema SCHEMA =
            Schema.of(
                    Schema.Field.of("id", Schema.FieldType.INT32),
                    Schema.Field.of("text", Schema.FieldType.STRING),
                    Schema.Field.of("key", Schema.FieldType.STRING),
                    Schema.Field.of("algorithm", Schema.FieldType.STRING));

    //Encrypt the text data using the key and algorithm info
    static class DoEncryptPojosFn extends DoFn<TextData, TextData> {
        @ProcessElement
        public void processEncryptTextElement(@Element TextData element, OutputReceiver<TextData> receiver){
            //TO DO : use real encryption algorithm
            String encryptedText = "@#"+ element.getText();

            receiver.output(new TextData(element.getId(), encryptedText, element.getKey(), element.getAlgorithm()));
        }
    }

    static class EncryptTextPojos extends PTransform<PCollection<TextData>, PCollection<TextData>>{

        @Override
        public PCollection<TextData> expand(PCollection<TextData> input) {
            PCollection<TextData> results = input.apply(ParDo.of(new DoEncryptPojosFn()));
            return results;
        }
    }

    //Make TextData object with the extracted text data
    static class DoExtractTextDataPojosFn extends DoFn<String, TextData>{
        @ProcessElement
        public void processExtractTextData(@Element String element, OutputReceiver<TextData> receiver){

            String[] inputs = element.split(",", -1);
            if(inputs.length == 4){
                TextData td = new TextData(Integer.parseInt(inputs[0].trim()), //id
                                    inputs[1].trim(), //text
                                    inputs[2].trim(),//key
                                    inputs[3].trim());//algorithm
                receiver.output(td);
            }


        }
    }

    //Convert input string from csv file to TextData collection
    static class ConvertInputDataPojos extends PTransform<PCollection<String>, PCollection<TextData>>{

        @Override
        public PCollection<TextData> expand(PCollection<String> line) {
            PCollection<TextData> inputData = line.apply(ParDo.of(new DoExtractTextDataPojosFn()));

            return inputData;
        }
    }

    static class FormatAsTextPojosFn extends SimpleFunction<TextData, String>{
        @Override
        public String apply(TextData input) {
            return input.toString();
        }
    }

    //Encrypt the text data using the key and algorithm info
    static class DoEncryptFn extends DoFn<Row, Row> {
        @ProcessElement
        public void processEncryptTextElement(@Element Row element, OutputReceiver<Row> receiver){
            //TO DO : use real encryption algorithm
            String encryptedText = "@#"+ element.getString("text");

            Row row = Row.withSchema(SCHEMA)
                    .addValues(element.getInt32("id"),
                            encryptedText,
                            element.getString("key"),
                            element.getString("algorithm"))
                    .build();//algorithm

            receiver.output(row);
        }
    }
    static class EncryptText extends PTransform<PCollection<Row>, PCollection<Row>>{

        @Override
        public PCollection<Row> expand(PCollection<Row> input) {
            PCollection<Row> results = input.apply(ParDo.of(new DoEncryptFn()));
            return results;
        }
    }
    //Make TextData object with the extracted text data
    static class DoExtractTextDataFn extends DoFn<String, Row>{
        @ProcessElement
        public void processExtractTextData(@Element String element, OutputReceiver<Row> receiver){

            String[] inputs = element.split(",", -1);
            if(inputs.length == 4){
                Row row = Row.withSchema(SCHEMA)
                        .withFieldValue("id",Integer.parseInt(inputs[0].trim()))//id
                        .withFieldValue("text",inputs[1].trim()) //text
                        .withFieldValue("key",inputs[2].trim())//key
                        .withFieldValue("algorithm",inputs[3].trim())
                        .build();//algorithm
                receiver.output(row);
            }


        }
    }
    static class ConvertInputData extends PTransform<PCollection<String>, PCollection<Row>>{

        @Override
        public PCollection<Row> expand(PCollection<String> line) {
            PCollection<Row> inputData = line.apply(ParDo.of(new DoExtractTextDataFn()));

            return inputData;
        }
    }

    //return string ( "id,text,key,algorithm" ) from TextData object
    static class FormatAsTextFn extends SimpleFunction<Row, String>{
        @Override
        public String apply(Row input) {
            String result = input.getInt32("id") + ","
                            + input.getString("text") + ","
                            + input.getString("key") + ","
                            + input.getString("algorithm");
            return result;
        }
    }
    static void runTextEncryption(TextEncryptionOptions options){
        Pipeline p = Pipeline.create(options);
        System.out.println("CryptoOperation:"+options.getCryptoOperation());
        p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply("ConvertInputDataToObject", new ConvertInputData())
                .setRowSchema(SCHEMA)
               .apply("Text Encryption", new EncryptText())
                .setRowSchema(SCHEMA)
                .apply("TextData to String(for csv)", MapElements.via(new FormatAsTextFn()))
                .apply("WriteEncryptText", TextIO.write().to(options.getOutput()).withoutSharding().withSuffix(".csv"));
                //.apply("WriteEncryptText", TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();
    }

    public static void main(String[] args){
        TextEncryptionOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TextEncryptionOptions.class);
        System.out.println("Hello TextEncryption");
        runTextEncryption(options);
    }
}
