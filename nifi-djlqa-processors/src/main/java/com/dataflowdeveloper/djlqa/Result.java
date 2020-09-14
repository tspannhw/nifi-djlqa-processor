package com.dataflowdeveloper.djlqa;

import java.io.Serializable;
import java.util.StringJoiner;

/**
 * QA Bert
 * @author tspann
 */
public class Result implements Serializable {

    private String prediction;
    private String errorString;
    private String question;
    private String paragraph;

    public String getPrediction() {
        return prediction;
    }

    public void setPrediction(String prediction) {
        this.prediction = prediction;
    }

    public String getErrorString() {
        return errorString;
    }

    public void setErrorString(String errorString) {
        this.errorString = errorString;
    }

    public String getQuestion() {
        return question;
    }

    public void setQuestion(String question) {
        this.question = question;
    }

    public String getParagraph() {
        return paragraph;
    }

    public void setParagraph(String paragraph) {
        this.paragraph = paragraph;
    }

    public Result() {
        super();
    }

    public Result(String prediction, String errorString, String question, String paragraph) {
        super();
        this.prediction = prediction;
        this.errorString = errorString;
        this.question = question;
        this.paragraph = paragraph;
    }

    @Override
    public String toString() {
        return new StringJoiner( ", ", Result.class.getSimpleName() + "[", "]" )
                .add( "prediction='" + prediction + "'" )
                .add( "errorString='" + errorString + "'" )
                .add( "question='" + question + "'" )
                .add( "paragraph='" + paragraph + "'" )
                .toString();
    }


}