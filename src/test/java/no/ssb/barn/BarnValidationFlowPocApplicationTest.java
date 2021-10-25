package no.ssb.barn;

import com.google.api.services.dataflow.model.Job;
import com.google.cloud.pubsublite.*;
import com.google.cloud.pubsublite.proto.Subscription;
import org.apache.beam.sdk.testing.TestPipeline;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.dataflow.model.ListJobsResponse;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.api.services.dataflow.Dataflow;
import org.junit.*;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.api.gax.paging.Page;
import com.google.api.client.json.gson.GsonFactory;
import java.util.concurrent.TimeUnit;
import java.nio.charset.StandardCharsets;
import org.apache.beam.sdk.values.KV;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.google.common.truth.Truth.assertThat;
import static junit.framework.TestCase.assertNotNull;

public class BarnValidationFlowPocApplicationTest {
    @Rule
    public final TestPipeline testPipeline = TestPipeline.create();

    private static final String projectId = System.getenv("GOOGLE_CLOUD_PROJECT");
    private static final String cloudRegion = "europe-north1";
    private static final char zoneId = 'b';
    private static final String suffix = UUID.randomUUID().toString();
    private static final String topicId = "proto-ham-test-1";
    private static final String subscriptionId = "proto-ham-test-1-sub" + suffix;
    private static final String bucketName = "hams_ssb_proto";
    private static final String directoryPrefix = "results/" + suffix;
    private static final String jobName = "pubsub-dataflow-job-" + suffix;

    private static final Storage storage =
        StorageOptions.newBuilder().setProjectId(projectId).build().getService();

    private static final TopicPath topicPath =
        TopicPath.newBuilder()
            .setProject(ProjectId.of(projectId))
            .setLocation(CloudZone.of(CloudRegion.of(cloudRegion), zoneId))
            .setName(TopicName.of(topicId))
            .build();

    private static final SubscriptionPath subscriptionPath =
        SubscriptionPath.newBuilder()
            .setLocation(CloudZone.of(CloudRegion.of(cloudRegion), zoneId))
            .setProject(ProjectId.of(projectId))
            .setName(SubscriptionName.of(subscriptionId))
            .build();

    private static final Subscription subscription =
        Subscription.newBuilder()
            .setDeliveryConfig(
                Subscription.DeliveryConfig.newBuilder()
                    .setDeliveryRequirement(Subscription.DeliveryConfig.DeliveryRequirement.DELIVER_IMMEDIATELY))
            .setName(subscriptionPath.toString())
            .setTopic(topicPath.toString())
            .build();

    private static final AdminClientSettings adminClientSettings =
        AdminClientSettings.newBuilder().setRegion(CloudRegion.of(cloudRegion)).build();

    private static void requireEnvVar(String varName) {
        assertNotNull(
            "Environment variable " + varName + " is required to perform these tests.",
            System.getenv(varName));
    }

    @BeforeClass
    public static void checkRequirements() {
        requireEnvVar("GOOGLE_CLOUD_PROJECT");
    }

    @Before
    public void setUp() throws Exception {
        // Create a subscription that reads from the entire message backlog in the topic.
        try (AdminClient adminClient = AdminClient.create(adminClientSettings)) {
            Subscription response = adminClient.createSubscription(subscription).get();
            System.out.println(response.getAllFields() + " created successfully.");
        }
    }

    @After
    public void tearDown() throws Exception {
        // Delete the test subscription.
        try (AdminClient adminClient = AdminClient.create(adminClientSettings)) {
            adminClient.deleteSubscription(subscriptionPath).get();
            System.out.println("Deleted subscription: " + subscriptionPath);
        }

        // Delete the output files.
        Page<Blob> blobs = storage.list(bucketName, Storage.BlobListOption.prefix(directoryPrefix));

        for (Blob blob : blobs.iterateAll()) {
            storage.delete(bucketName, blob.getName());
            System.out.println("Deleted a file: " + blob.getName());
        }

        // Stop the Dataflow job.
        NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

        GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
        HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);

        Dataflow dataflow =
                new Dataflow.Builder(httpTransport, GsonFactory.getDefaultInstance(), requestInitializer)
                        .build();

        // Match Dataflow job of the same job name and cancel it.
        ListJobsResponse jobs =
                dataflow.projects().locations().jobs().list(projectId, cloudRegion).execute();

        try {
            jobs.getJobs()
                .forEach(
                    job -> {
                        if (job.getName().equals(jobName)) {
                            String jobId = job.getId();
                            try {
                                dataflow
                                    .projects()
                                    .locations()
                                    .jobs()
                                    .update(
                                        projectId,
                                        cloudRegion,
                                        jobId,
                                        new Job().setRequestedState("JOB_STATE_CANCELLED"))
                                    .execute();
                                System.out.println("Cancelling Dataflow job: " + jobId);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    });
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testPubsubliteToGcs() throws InterruptedException, IOException {
        // Run the pipeline on Dataflow as instructed in the README.
        BarnValidationFlowPocApplication.main(
            new String[] {
                "--subscription=" + subscriptionPath.toString(),
                "--output=gs://" + bucketName + "/" + directoryPrefix + "/output",
                "--windowSize=1",
                "--runner=DataflowRunner",
                "--project=" + projectId,
                "--region=" + cloudRegion,
                "--tempLocation=gs://" + bucketName + "/temp",
                "--jobName=" + jobName
            });

        // Wait 10 minute for the Dataflow job.
        TimeUnit.MINUTES.sleep(10);

        // Check for output files.
        Page<Blob> blobs = storage.list(bucketName, Storage.BlobListOption.prefix(directoryPrefix));

        List<KV<String, String>> desiredOutput = new ArrayList<KV<String, String>>();
        for (Blob blob : blobs.iterateAll()) {
            String fileName = blob.getName();
            String content = new String(blob.getContent(), StandardCharsets.UTF_8);
            System.out.println("Found a file: " + fileName);
            System.out.println("Has content: " + content);

            desiredOutput.add(KV.of(fileName, content));
        }

        assertThat(
            desiredOutput.contains(
                KV.of(directoryPrefix + "/output-17:08-17:09-0-of-1", "Hello world!\t1619197725")));
        assertThat(
            desiredOutput.contains(
                KV.of(directoryPrefix + "/output-17:09-17:10-0-of-1", "Hello world!\t1619197759")));

    }
}