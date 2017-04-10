package nl.surfsara.newsreader.pipeline;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PipelineComponentDescription {
    public String name;
    public Long timeout;
    public Integer numErrorLines;
    @JsonProperty(value = "class")
    public String clazz;
}
