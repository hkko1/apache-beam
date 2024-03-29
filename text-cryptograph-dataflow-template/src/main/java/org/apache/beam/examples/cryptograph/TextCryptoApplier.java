package org.apache.beam.examples.cryptograph;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

public class TextCryptoApplier extends PTransform<PCollection<Row>, PCollection<Row>> {


    private PipelineOptions options;
    private String transformation;
    private TextCryptoApplier(
            String transformation,
            TextCryptoTransformPipeline.TextCryptoTransformOptions options){
        this.transformation = transformation;
        this.options = options;
    }

    public static TextCryptoApplier of(
            String transformation,
            TextCryptoTransformPipeline.TextCryptoTransformOptions options){
        return new TextCryptoApplier(transformation,options);

    }

    static class DoTransformFn extends DoFn<Row, Row> {

        private String transformation;
        private Schema schema;
        public DoTransformFn(String transformation, Schema schema){

            this.transformation = transformation;
            this.schema = schema;
        }
        @ProcessElement
        public void processEncryptTextElement(@Element Row element, OutputReceiver<Row> receiver){
            //TO DO : use real encryption algorithmS
            //String encryptedText = "@#"+ element.getString("text");
            System.out.println("TextCryptoApplier: " + element.getInt32("id").toString());
            String transformedText = "";

            try{
                if(transformation.equals("encryption")){
                    transformedText = Cryptographer.encryptMessage(element.getString("text"),
                            element.getString("key"),
                            element.getString("transformation"));
                }else{
                    transformedText = Cryptographer.decryptMessage(element.getString("text"),
                            element.getString("key"),
                            element.getString("transformation"));
                }

            }catch(Exception e){
                System.out.println(transformation + "("+element.getString("transformation")+") Failed: " + e);
            }


            boolean isTransformed = false;

            if(transformedText != null && transformedText.length() != 0){
                isTransformed = true;
            }

            Row row = Row.withSchema(schema)
                    .addValues(element.getInt32("id"),
                            new String(transformedText),
                            element.getString("key"),
                            element.getString("transformation"),
                            isTransformed)
                    .build();//transformation

            receiver.output(row);
        }
    }

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        Schema inputSchema = input.getSchema();
        PCollection<Row> results = input.apply(ParDo.of(new DoTransformFn(transformation, inputSchema)))
                                        .setRowSchema(inputSchema);
        return results;
    }
}
