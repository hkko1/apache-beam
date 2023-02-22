package org.apache.beam.examples.cryptograph;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

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

    //Encrypt the text data using the key and algorithm info
    static class DoEncryptFn extends DoFn<TextData, TextData> {
        @ProcessElement
        public void processEncryptTextElement(@Element TextData element, OutputReceiver<TextData> receiver){
            //TO DO : use real encryption algorithm
            String encryptedText = "@#"+ element.getText();

            receiver.output(new TextData(element.getId(), encryptedText, element.getKey(), element.getAlgorithm()));
        }
    }

    static class EncryptText extends PTransform<PCollection<TextData>, PCollection<TextData>>{

        @Override
        public PCollection<TextData> expand(PCollection<TextData> input) {
            PCollection<TextData> results = input.apply(ParDo.of(new DoEncryptFn()));
            return results;
        }
    }

    //Make TextData object with the extracted text data
    static class DoExtractTextDataFn extends DoFn<String, TextData>{
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
    static class ConvertInputData extends PTransform<PCollection<String>, PCollection<TextData>>{

        @Override
        public PCollection<TextData> expand(PCollection<String> line) {
            PCollection<TextData> inputData = line.apply(ParDo.of(new DoExtractTextDataFn()));

            return inputData;
        }
    }

    //return string ( "id,text,key,algorithm" ) from TextData object
    static class FormatAsTextFn extends SimpleFunction<TextData, String>{
        @Override
        public String apply(TextData input) {
            return input.toString();
        }
    }
    static void runTextEncryption(TextEncryptionOptions options){
        Pipeline p = Pipeline.create(options);
        System.out.println("CryptoOperation:"+options.getCryptoOperation());
        p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply("ConvertInputDataToObject", new ConvertInputData())
               .apply("Text Encryption", new EncryptText())
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
