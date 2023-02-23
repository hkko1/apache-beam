package org.apache.beam.examples.cryptograph;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class TextData {
    int id;
    String text;
    String key;
    String transformation;

    public TextData(){
        //default
    }
    public TextData(int id, String text, String key, String transformation) {
        this.id = id;
        this.text = text;
        this.key = key;
        this.transformation = transformation;
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

    public String getTransformation() {
        return transformation;
    }

    public void setTransformation(String transformation) {
        this.transformation = transformation;
    }

    @Override
    public String toString() {

        String outputStr = id +"," + text +"," + key +"," + transformation;
        return outputStr;
    }
}
