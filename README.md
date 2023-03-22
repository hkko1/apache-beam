# Apache-Beam
-  word-count-beam : [example from apache beam](https://beam.apache.org/get-started/wordcount-example/)
-  text-cryptograph: apache-beam study ( input: csv file (text) , output: csv file (encrypted text))
- text-cryptograph-dataflow-template : adjust GCP dataflow template to `text-cryptograph`

## Apache beam Row 
- need to check [Schema](https://beam.apache.org/documentation/programming-guide/#what-is-a-schema)

## Multiple transforms process the same PCollection
- You can use the same PCollection as input for multiple transforms without consuming the input or altering it. ( [More](https://beam.apache.org/documentation/pipelines/design-your-pipeline/#multiple-transforms-process-the-same-pcollection))

## PTransform  vs ParDo
- ParDo: The ParDo process paradigm is similar to the "Map" phase of a Map/Shuffle/Reduce-style algorithm- a ParDo transform considers `each element` in the input PCollection, performs some `prcessing function (your user code)` on that element, and emit `zero, one, or multiple elements` to an output PCollection. ([More...](https://beam.apache.org/documentation/programming-guide/#pardo))
- PTransform  : A PTransform represents a data processing operation, or a step, in your pipeline. Every PTransform takes `one or more PCollection objects` as input, performs a processing function that you provide on the elements of that PCollection , and produces `zero or more output PCollection objects`.

# [Dataflow CDC Example](https://github.com/GoogleCloudPlatform/DataflowTemplates/tree/main/v2/cdc-parent)
- keyword: 

# ETC
- ...