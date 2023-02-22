package org.apache.beam.examples.cryptograph;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class TextData {
    int id;
    String text;
    String key;
    String algorithm;

    public TextData(){
        //default
    }
    public TextData(int id, String text, String key, String algorithm) {
        this.id = id;
        this.text = text;
        this.key = key;
        this.algorithm = algorithm;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    @Override
    public String toString() {

        String outputStr = id +"," + text +"," + key +"," + algorithm;
        return outputStr;
    }
}
