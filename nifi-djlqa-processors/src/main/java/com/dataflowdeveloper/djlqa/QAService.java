package com.dataflowdeveloper.djlqa;

import ai.djl.Application;
import ai.djl.ModelException;
import ai.djl.inference.Predictor;
import ai.djl.modality.nlp.qa.QAInput;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelZoo;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import ai.djl.translate.TranslateException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import ai.djl.pytorch.zoo.nlp.qa.*;
import ai.djl.Application;
import ai.djl.MalformedModelException;
import ai.djl.ModelException;
import ai.djl.inference.Predictor;
import ai.djl.modality.Classifications;
import ai.djl.repository.zoo.Criteria;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ModelZoo;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.training.util.ProgressBar;
import ai.djl.translate.TranslateException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example of inference using BertQA.
 *
 * <p>See:
 *
 * <ul>
 *   <li>the <a href="https://github.com/awslabs/djl/blob/master/jupyter/BERTQA.ipynb">jupyter
 *       demo</a> with more information about BERT.
 *   <li>the <a
 *       href="https://github.com/awslabs/djl/blob/master/examples/docs/BERT_question_and_answer.md">docs</a>
 *       for information about running this example.
 * </ul>
 */
// scalastyle:off
// scalastyle:on

public class QAService {

    /**
     * trained on https://gluon-nlp.mxnet.io/model_zoo/bert/index.html
     * book_corpus_wiki_en_uncasedwiki
     * https://rajpurkar.github.io/SQuAD-explorer/explore/v2.0/dev/Steam_engine.html
     *
     * @param question
     * @param paragraph
     * @return
     * @throws IOException
     * @throws TranslateException
     * @throws ModelException
     */
    public Result predict(String question, String paragraph) throws IOException, TranslateException, ModelException {

        Result result = new Result();

        if (question == null || question.trim().length()<=0 ||
            paragraph == null | paragraph.trim().length()<=0) {
            return result;
        }

        String prediction = null;
        QAInput input = null;
        Criteria<QAInput, String> criteria = null;
        try {
            input = new QAInput(question.toLowerCase(), paragraph.toLowerCase());

            criteria = Criteria.builder()
                    .optApplication( Application.NLP.QUESTION_ANSWER)
                    .setTypes( QAInput.class, String.class)
                    .optFilter("backbone", "bert")
                    .optEngine( "PyTorch" )
                    .optProgress(new ProgressBar())
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            result.setErrorString( e.getLocalizedMessage() );
            return result;
        }

        try (ZooModel<QAInput, String> model = ModelZoo.loadModel(criteria)) {
            try (Predictor<QAInput, String> predictor = model.newPredictor()) {
                prediction = predictor.predict( input );
                if ( prediction != null ) {
                    result.setPrediction( prediction );
                }
            }
            catch(Throwable t) {
                t.printStackTrace();
                result.setErrorString( t.getLocalizedMessage() );
            }
        }
        catch(Throwable t) {
            t.printStackTrace();
            result.setErrorString( t.getLocalizedMessage() );
        }

        if ( result.getPrediction() == null) {
            result.setPrediction( "NiFi DJL cannot determine the answer." );
        }
        return result;
    }
}
