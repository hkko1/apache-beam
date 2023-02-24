package org.apache.beam.examples.cryptograph;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TextCryptoPCollectionFetchers{

    private TextCryptoTransformPipeline.TextCryptoTransformOptions options;
    public static TextCryptoPCollectionFetchers create(
            TextCryptoTransformPipeline.TextCryptoTransformOptions options) {
        return new TextCryptoPCollectionFetchers(options);
    }

    TextCryptoPCollectionFetchers(TextCryptoTransformPipeline.TextCryptoTransformOptions options){
        this.options = options;
    }

    public Map<String, PCollection<Row>> convertInputPcollections(Pipeline p) {
        Map<String, PCollection<Row>> result = new HashMap<>();

        Map<String, Schema> csvToSchema = new HashMap<String, Schema>();
        csvToSchema.put(options.getCryptoOperation(), TextCryptoTransformPipeline.SCHEMA);

//        if (csvToSchema == null) {
//            throw new RuntimeException(
//                    "Unable to use textInput schemas. Can't build the pipeline.");
//        }

        PCollection<String> csvData;
        if(options.getCryptoOperation().equals("encryption")){
            csvData = p.apply("ReadLines", TextIO.read().from(options.getInputFile()));
        }else if(options.getCryptoOperation().equals("decryption")){ //decryption
            csvData = p.apply("ReadLines", TextIO.read().from(options.getInputFile()));
        }else{
            System.out.println("===Error: CryptoOperation option is wrong. Please use 'encryption' or 'decryption === ");
            return null;
        }

        for (Map.Entry<String, Schema> csvAndSchema : csvToSchema.entrySet()) {
            PCollection<Row> textInputData =
                    csvDecode(csvData, csvAndSchema.getKey(), csvAndSchema.getValue());
            result.put(csvAndSchema.getKey(), textInputData);
        }

        return result;
    }

    private static PCollection<Row> csvDecode(PCollection<String> input, final String operation, Schema csvSchema ){


        return input.apply("csv decode", DecodeRows.withSchema(csvSchema));
    }

}