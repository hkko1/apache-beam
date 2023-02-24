package org.apache.beam.examples.cryptograph;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class DecodeRows extends PTransform<PCollection<String>, PCollection<Row>> {

    private final Schema schema;
    private DecodeRows(Schema csvSchema){
        this.schema = csvSchema;
    }

    public static DecodeRows withSchema(Schema csvSchema){
        return new DecodeRows((csvSchema));
    }

    @Override
    public PCollection<Row> expand(PCollection<String> line) {
        PCollection<Row> inputData = line
                .apply(ParDo.of(new DoExtractTextDataFn(schema)))
                .setRowSchema(schema);

        return inputData;
    }

    static class DoExtractTextDataFn extends DoFn<String, Row> {

        private Schema schema;
        public DoExtractTextDataFn(Schema schema){
            this.schema = schema;
        }
        @ProcessElement
        public void processExtractTextData(@Element String element, OutputReceiver<Row> receiver){
            System.out.println("input_element:"+element);
            String[] inputs = element.split(",", -1);
            System.out.println("input:(id:"+Integer.parseInt(inputs[0].trim()) + " ,text: "+inputs[1].trim()+", key:"+inputs[2].trim() +", transformation: "+inputs[3].trim());
            if(inputs.length == 4){
                Row row = Row.withSchema(schema)
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
}
