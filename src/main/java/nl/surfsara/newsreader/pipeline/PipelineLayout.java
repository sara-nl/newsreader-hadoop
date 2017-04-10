/**
 * Copyright 2014 SURFsara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package nl.surfsara.newsreader.pipeline;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import nl.surfsara.newsreader.pipeline.modules.Module;
import nl.surfsara.newsreader.pipeline.modules.PipelineStep;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

/**
 * Class that reads and parses a layout file. The layout file is json document
 * that describes the steps in the pipeline to execute. Please see the json
 * schema for more details on this file.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public class PipelineLayout {
	private String pipelineversion;
	private String pipelineid;
	private String description;
	private ArrayList<PipelineStep> steps;
	private String layoutFile;

	public PipelineLayout(String layoutFile) throws FileNotFoundException, IOException, ClassNotFoundException {
		this.layoutFile = layoutFile;
		init(layoutFile);
	}

	private void init(String layoutFile) throws FileNotFoundException, IOException, ClassNotFoundException {

		ObjectMapper mapper;
		if (layoutFile.endsWith(".yaml") || layoutFile.endsWith(".yml")) {
			mapper = new ObjectMapper(new YAMLFactory());
		} else {
			mapper = new ObjectMapper();
		}

		PipelineDescription pd = mapper.readValue(new File(layoutFile), PipelineDescription.class);

		pipelineid = pd.id;
		pipelineversion = pd.version;
		description = pd.description;
		List<PipelineComponentDescription> layout = pd.layout;
		steps = new ArrayList<>();
		for (PipelineComponentDescription pcd: layout) {
			String name = pcd.name;
			String className = pcd.clazz;
			long timeOut = pcd.timeout;
			int numErrorLine = pcd.numErrorLines;

			@SuppressWarnings("unchecked")
			PipelineStep step = new PipelineStep(name, ((Class<? extends Module>) Class.forName(className)), timeOut, numErrorLine);
			steps.add(step);
		}
	}

	public String getPipelineversion() {
		return pipelineversion;
	}

	public String getDescription() {
		return description;
	}

	public ArrayList<PipelineStep> getSteps() {
		return steps;
	}

	public String getPipelineid() {
		return pipelineid;
	}

}
