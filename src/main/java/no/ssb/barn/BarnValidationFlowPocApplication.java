package no.ssb.barn;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import no.ssb.barn.validation.BarnValidationOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.xml.XmlIO;
import org.apache.beam.sdk.io.xml.JAXBCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.ssb.barn.validation.BarnValidationController;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.hamcrest.text.IsEmptyString;
import org.json.JSONException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;

public class BarnValidationFlowPocApplication {

    public static void main(String[] args) throws IOException  {
        BarnValidationOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(BarnValidationOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline//.apply("Read XML file from inputTopic", // string? element butter? Stringbutter?
                //    XmlIO.read().from(options.getInputTopic()).withCharset(StandardCharsets.UTF_8))
                .apply(TextIO.read().from(options.getInputTopic()))
                .apply("convert to JSON and check dato", ParDo.of(new DoFn<String, String>() {
                    ObjectMapper jacksonObjMapper = new ObjectMapper();

                    public void processElement(ProcessContext c) throws Exception {
                        String Barnevern = (String) c.element();

                        JsonNode jsonNode = jacksonObjMapper.readTree(Barnevern);

                        try {
                            if (jsonNode.has("StartDato") && !jsonNode.isEmpty()) {
                                if (jsonNode.has("SluttDato") && !jsonNode.isEmpty()) {
                                    int compareVal = jsonNode.get("StartDato").textValue().compareTo(jsonNode.get("SluttDato").textValue());

                                }
                            }
                        } catch (JSONException e) {
                            e.printStackTrace();
                        }

                        String jsonString = jsonNode.asText();
                        c.output(jsonString);
                    }
                }))
//                .apply("key-value string", WithKeys.of(new SerializableFunctions<K, V>() {
//
//                    public String apply(String s) {
//                        return s;
//                    }
//                }))
                .apply("write JSON to Storage", TextIO.write().to(options.getTextWritePrefix()).withSuffix(".json"));

        pipeline.run();
    }
}
