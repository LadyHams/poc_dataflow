package no.ssb.barn.validation;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface BarnValidationOptions extends PipelineOptions, StreamingOptions {
    @Description("The Cloud Pub/Sub topic to read from.")
    @Default.String("proto-ham-test-1")
    @Validation.Required
    String getInputTopic();

    void setInputTopic(String value);

    // Optional argument with a default value.
    @Description("Google Cloud Storage file pattern glob of the file(s) to read from")
    @Default.String("gs://hams_ssb_proto/input/sample.xml")
    ValueProvider<String> getInputFile();

    void setInputFile(ValueProvider<String> value);

    // Required argument (made required via the metadata file).
    @Description("Google Cloud Storage bucket to store the outputs")
    @Default.String("gs://hams_ssb_proto/output/")
    ValueProvider<String> getOutputBucket();

    void setOutputBucket(ValueProvider<String> value);

    // Required argument (made required via the metadata file).
    @Description("Google Cloud Storage bucket to store the outputs")
    @Default.String("filePrefix_in_storage")
    ValueProvider<String> getTextWritePrefix();

    void setTextWritePrefix(ValueProvider<String> value);


}
