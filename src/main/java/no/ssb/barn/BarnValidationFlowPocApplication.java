package no.ssb.barn;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.barn.validation.BarnValidationOptions;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.json.JSONException;

import java.io.IOException;

public class BarnValidationFlowPocApplication {

    private static final Logger LOG = LoggerFactory.getLogger(BarnValidationFlowPocApplication.class);

    public static void main(String[] args) throws IOException  {
        BarnValidationOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(BarnValidationOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from(options.getInputTopic()))
                .apply("convert to JSON and check dato", ParDo.of(new DoFn<String, String>() {
                    ObjectMapper jacksonObjMapper = new ObjectMapper();

                    public void processElement(ProcessContext c) throws Exception {
                        String Barnevern = (String) c.element();

                        JsonNode jsonNode = jacksonObjMapper.readTree(Barnevern);

                        try {
                            if (jsonNode.has("StartDato") && !jsonNode.isEmpty()) {
                                if (jsonNode.has("SluttDato") && !jsonNode.isEmpty()) {
                                    int compareVal = jsonNode.get("StartDato").textValue().compareTo(jsonNode.get("SluttDato").textValue());

                                    LOG.info("check dato interval", compareVal);
                                }
                            }
                        } catch (JSONException e) {
                            e.printStackTrace();
                        }

                        String jsonString = jsonNode.asText();
                        c.output(jsonString);
                    }
                }))
                .apply("Write results", TextIO.write().to(NestedValueProvider.of(
                        options.getOutputBucket(),
                        (String bucket) -> String.format("gs://hams_ssb_proto/results/outputs", bucket)
                )));

        pipeline.run();
    }
}
