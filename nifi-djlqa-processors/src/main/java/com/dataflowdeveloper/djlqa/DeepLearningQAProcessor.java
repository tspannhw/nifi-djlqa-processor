package com.dataflowdeveloper.djlqa;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;

@EventDriven
@SupportsBatching
@SideEffectFree
@Tags({ "djl", "inference", "models", "classification", "computer vision", "image", "ssd", "object detection", "Java Deep Learning", "deep learning" })
@CapabilityDescription("Run BERT QA Deep Learning Models with DJL")
@SeeAlso({})
@WritesAttributes({ @WritesAttribute(attribute = "className", description = "The image x, y, bounding box, probabilities and class name") })
/**
 *
 * @author tspann  Timothy Spann
 *
 */
public class DeepLearningQAProcessor extends AbstractProcessor {

    // Input variables
    public static final String QUESTION_NAME = "question";
    public static final String PARAGRAPH_NAME = "paragraph";
    public static final String OUTPUT_PREDICTION = "prediction";
    public static final String OUTPUT_ERROR = "error";

    // properties
    public static final PropertyDescriptor QUESTION = new PropertyDescriptor.Builder().name( QUESTION_NAME )
            .description("Question").required(true).defaultValue("What")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor PARAGRAPH = new PropertyDescriptor.Builder().name(PARAGRAPH_NAME)
            .description("Paragraph").required(true).defaultValue("...")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully determined image.").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to determine image.").build();
    public static final String FILENAME = "filename";

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    // DL4J Deep Learning Service
    private QAService service;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(QUESTION);
        descriptors.add(PARAGRAPH);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        service = new QAService();
        return;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            flowFile = session.create();
        }
        try {
            String question = flowFile.getAttribute(QUESTION_NAME);
            if (question == null) {
                question = context.getProperty(QUESTION_NAME).evaluateAttributeExpressions(flowFile).getValue();
            }

            String paragraph = context.getProperty(PARAGRAPH_NAME).evaluateAttributeExpressions(flowFile).getValue();
            if (paragraph == null) {
                paragraph = flowFile.getAttribute(PARAGRAPH_NAME);
            }

            Map<String, String> attributes = flowFile.getAttributes();
            Map<String, String> attributesClean = new HashMap<>();
            
            Result result = service.predict( question, paragraph );

            attributesClean.put(OUTPUT_PREDICTION, result.getPrediction());
            attributesClean.put(OUTPUT_ERROR, result.getErrorString());

            if (attributes.size() == 0) {
                session.transfer(flowFile, REL_FAILURE);
            } else {
                flowFile = session.putAllAttributes(flowFile, attributesClean);
                session.transfer(flowFile, REL_SUCCESS);
            }
            session.commit();
        } catch (final Throwable t) {
            getLogger().error("Unable to process Deep Learning BERT QA DL " + t.getLocalizedMessage());
            getLogger().error("{} failed to process due to {}; rolling back session", new Object[] { this, t });
        }
    }
}
