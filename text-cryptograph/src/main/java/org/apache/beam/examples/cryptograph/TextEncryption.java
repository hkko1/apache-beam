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
                    Schema.Field.of("transformation", Schema.FieldType.STRING),
                    Schema.Field.of("isTransformed", Schema.FieldType.BOOLEAN));

    //Encrypt the text data using the key and transformation algorithm info
    static class DoEncryptPojosFn extends DoFn<TextData, TextData> {
        @ProcessElement
        public void processEncryptTextElement(@Element TextData element, OutputReceiver<TextData> receiver){
            //TO DO : use real encryption algorithm
            String encryptedText = "@#"+ element.getText();

            receiver.output(new TextData(element.getId(), encryptedText, element.getKey(), element.getTransformation()));
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
                                    inputs[3].trim());//transformation
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
            //TO DO : use real encryption algorithmS
            //String encryptedText = "@#"+ element.getString("text");

            String encryptedText = "";

            try{
                encryptedText = Cryptographer.encryptMessage(element.getString("text"),
                                                            element.getString("key"),
                                                            element.getString("transformation"));
            }catch(Exception e){
                System.out.println("Encryption("+element.getString("transformation")+") Failed: " + e);
            }


            boolean isTransformed = false;

            if(encryptedText != null && encryptedText.length() != 0){
                isTransformed = true;
            }

            Row row = Row.withSchema(SCHEMA)
                    .addValues(element.getInt32("id"),
                            new String(encryptedText),
                            element.getString("key"),
                            element.getString("transformation"),
                            isTransformed)
                    .build();//transformation

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

    //Encrypt the text data using the key and algorithm info
    static class DoDecryptFn extends DoFn<Row, Row> {
        @ProcessElement
        public void processDecryptTextElement(@Element Row element, OutputReceiver<Row> receiver){
            //TO DO : use real encryption algorithmS
            //String encryptedText = "@#"+ element.getString("text");

            String decryptedText = "";

            try{
                decryptedText = Cryptographer.decryptMessage(element.getString("text"),
                        element.getString("key"),
                        element.getString("transformation"));
            }catch(Exception e){
                System.out.println("Decryption("+element.getString("transformation")+") Failed: " + e);
            }


            boolean isTransformed = false;

            if(decryptedText != null && decryptedText.length() != 0){
                isTransformed = true;
            }


            Row row = Row.withSchema(SCHEMA)
                    .addValues(element.getInt32("id"),
                            new String(decryptedText),
                            element.getString("key"),
                            element.getString("transformation"),
                            isTransformed)
                    .build();//transformation

            receiver.output(row);
        }
    }
    static class DecryptText extends PTransform<PCollection<Row>, PCollection<Row>>{

        @Override
        public PCollection<Row> expand(PCollection<Row> input) {
            PCollection<Row> results = input.apply(ParDo.of(new DoDecryptFn()));
            return results;
        }
    }
    //Make TextData object with the extracted text data
    static class DoExtractTextDataFn extends DoFn<String, Row>{
        @ProcessElement
        public void processExtractTextData(@Element String element, OutputReceiver<Row> receiver){
            System.out.println("input_element:"+element);
            String[] inputs = element.split(",", -1);
            System.out.println("input:(id:"+Integer.parseInt(inputs[0].trim()) + " ,text: "+inputs[1].trim()+", key:"+inputs[2].trim() +", transformation: "+inputs[3].trim());
            if(inputs.length == 4){
                Row row = Row.withSchema(SCHEMA)
                        .withFieldValue("id",Integer.parseInt(inputs[0].trim()))//id
                        .withFieldValue("text",inputs[1].trim()) //text
                        .withFieldValue("key",inputs[2].trim())//key
                        .withFieldValue("transformation",inputs[3].trim())
                        .withFieldValue("isTransformed", false)
                        .build();//transformation
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

    static class DoExtractTransformationResultFn extends DoFn<Row, Boolean>{
        @ProcessElement
        public void processExtractTransformationResult(@Element Row element, OutputReceiver<Boolean> receiver){
            Boolean isTransformed = element.getBoolean("isTransformed");
            receiver.output(isTransformed);
        }
    }
    static class CountTransformedData extends PTransform<PCollection<Row>, PCollection<KV<Boolean, Long>>>{

        @Override
        public PCollection<KV<Boolean, Long>> expand(PCollection<Row> input) {
            PCollection<Boolean> transformedResult = input.apply(ParDo.of(new DoExtractTransformationResultFn()));

            // Count the number of times each word occurs.
            PCollection<KV<Boolean, Long>> counts = transformedResult.apply(Count.perElement());

            return counts;
        }
    }

    //return string ( "id,text,key,transformation" ) from TextData object
    static class FormatAsCsvFn extends SimpleFunction<Row, String>{
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
    static void runTextEncryption(TextEncryptionOptions options){
        Pipeline p = Pipeline.create(options);
        System.out.println("CryptoOperation:"+options.getCryptoOperation());

        //Multiple transforms process the same PCollection
        PCollection<Row> rowCollection;
        if(options.getCryptoOperation().equals("encryption")){
            rowCollection = p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                    .apply("ConvertInputDataToObject", new ConvertInputData())
                    .setRowSchema(SCHEMA)
                    .apply("Text Encryption", new EncryptText())
                    .setRowSchema(SCHEMA);
        }else if(options.getCryptoOperation().equals("decryption")){ //decryption
            rowCollection = p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                    .apply("ConvertInputDataToObject", new ConvertInputData())
                    .setRowSchema(SCHEMA)
                    .apply("Text Encryption", new DecryptText())
                    .setRowSchema(SCHEMA);
        }else{
            System.out.println("===Error: CryptoOperation option is wrong. Please use 'encryption' or 'decryption === ");
            return;
        }


        //Output : .csv file which has the transformed data
        rowCollection.apply("TextData to String(for csv)", MapElements.via(new FormatAsCsvFn()))
                .apply("WriteEncryptText", TextIO.write().to(options.getOutput()).withoutSharding().withSuffix(".csv"));
                //.apply("WriteEncryptText", TextIO.write().to(options.getOutput()));

        //Output: .txt file which has the number of transformed data
        rowCollection.apply("Count the number of transformed data", new CountTransformedData())
                .apply("BooleanResult to String(for txt)", MapElements.via(new FormatAsTextFn()))
                .apply("WriteCountResult", TextIO.write().to(options.getOutput()).withoutSharding().withSuffix(".txt"));


        p.run().waitUntilFinish();
    }

    public static void main(String[] args){
        TextEncryptionOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TextEncryptionOptions.class);
        System.out.println("Hello TextEncryption");
        runTextEncryption(options);
    }
}
