package com.dataflowdeveloper.djlqa;

import ai.djl.modality.Classifications;
import jdk.nashorn.internal.objects.NativeArray;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;


/**
 *
 */
public class DeepLearningQAProcessorTest {

    private static final String PARAGRAPH_TEST = "Apache NiFi was open-sourced as part of NSA's technology transfer program in 2014.   Development started in 2006.   It is currently supported by Cloudera";
    private static final String QUESTION_TEST = "Q: When did NiFi start?";

    // https://rajpurkar.github.io/SQuAD-explorer/explore/v2.0/dev/Black_Death.html?model=nlnet%20(single%20model)%20(Microsoft%20Research%20Asia)&version=v2.0
    private static final String PARAGRAPH_SQUAD = "The Black Death is thought to have originated in the arid plains of Central Asia, where it then travelled along the Silk Road, reaching Crimea by 1343. From there, it was most likely carried by Oriental rat fleas living on the black rats that were regular passengers on merchant ships. Spreading throughout the Mediterranean and Europe, the Black Death is estimated to have killed 30–60% of Europe's total population. In total, the plague reduced the world population from an estimated 450 million down to 350–375 million in the 14th century. The world population as a whole did not recover to pre-plague levels until the 17th century. The plague recurred occasionally in Europe until the 19th century.";
    private static final String QUESTION_SQUAD = "Where did the black death originate?";

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(DeepLearningQAProcessor.class);
    }


    private String pathOfResource(String name) throws URISyntaxException {
        URL r = this.getClass().getClassLoader().getResource(name);
        URI uri = r.toURI();
        return Paths.get(uri).toAbsolutePath().getParent().toString();
    }

    private void runAndAssertHappy() {
        testRunner.setValidateExpressionUsage(false);
        testRunner.run();
        testRunner.assertValid();

        testRunner.assertAllFlowFilesTransferred(DeepLearningQAProcessor.REL_SUCCESS);
        List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(DeepLearningQAProcessor.REL_SUCCESS);

        for (MockFlowFile mockFile : successFiles) {

            System.out.println("Size:" +             mockFile.getSize() ) ;
            Map<String, String> attributes =  mockFile.getAttributes();

            for (String attribute : attributes.keySet()) {
                System.out.println("Attribute:" + attribute + " = " + mockFile.getAttribute(attribute));
            }
        }
    }

    @Test
    public void testProcessorSQUADExample() {
    	testRunner.setProperty(DeepLearningQAProcessor.QUESTION_NAME, QUESTION_SQUAD);
    	testRunner.setProperty(DeepLearningQAProcessor.PARAGRAPH_NAME, PARAGRAPH_SQUAD);
    	testRunner.enqueue();

        // Must add valid url for integration test
        runAndAssertHappy();
    }

    @Test
    public void testProcessorSimple() {
        testRunner.setProperty(DeepLearningQAProcessor.QUESTION_NAME, QUESTION_TEST);
        testRunner.setProperty(DeepLearningQAProcessor.PARAGRAPH_NAME, PARAGRAPH_TEST);
        testRunner.enqueue();

        // Must add valid url for integration test
        runAndAssertHappy();
    }

}
