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
package nl.surfsara.newsreader.pipeline.cascading.flows;

import nl.surfsara.newsreader.pipeline.cascading.flows.udfs.FailedFilter;
import nl.surfsara.newsreader.pipeline.cascading.flows.udfs.InsertField;
import nl.surfsara.newsreader.pipeline.cascading.flows.udfs.RunModuleFunction;
import nl.surfsara.newsreader.pipeline.cascading.flows.udfs.StripField;
import nl.surfsara.newsreader.pipeline.cascading.flows.udfs.SuccessFilter;
import nl.surfsara.newsreader.pipeline.modules.ModuleFactory;

import org.apache.hadoop.io.Text;

import cascading.flow.FlowDef;
import cascading.pipe.Checkpoint;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.SequenceFile;
import cascading.scheme.hadoop.WritableSequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 * An implementation of the Newsreader NLP pipeline as a Cascading Flow.
 * 
 * Documents are read from sequence files(s) on HDFS and inserted into the tuple stream as the following tuples: <document name, document contents>.
 * The first function inserts the document failed field for each tuple: <document name, document contents, document failed>
 * The next functions execute the pipeline on the document contents field. Documents where docFailed has been set to true will not be processed by subsequent moduled.
 * Finally, the stream is split and failed and successful documents are stored in separate sinks on HDFS (again as sequence files with <key,value> = <document name, document contents>  
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public class NewsReaderFlow implements Flow {
	
	@Override
	public FlowDef getFlowDefinition(String inPath, String outPath, String errorPath) throws Exception {
		Fields pipelineFields = new Fields("docName", "docContent", "docFailed");
		Fields docFields = new Fields("docName", "docContent");
		
		//SequenceFile seq = new SequenceFile(docFields);
		WritableSequenceFile inseq = new WritableSequenceFile(docFields, Text.class, Text.class);

		@SuppressWarnings("rawtypes")
		Tap docTap = new Hfs(inseq, inPath);

		Pipe insertField = new Each("Insert docFailed", new InsertField(), Fields.RESULTS);
		
		Pipe EHUtok = new Each(new Pipe(ModuleFactory.EHUtok.getName(), insertField), pipelineFields, new RunModuleFunction(ModuleFactory.EHUtok), Fields.RESULTS);
		Pipe EHUpos = new Each(new Pipe(ModuleFactory.EHUpos.getName(), EHUtok), pipelineFields, new RunModuleFunction(ModuleFactory.EHUpos), Fields.RESULTS);
		Pipe VUAmultiwordtagger = new Each(new Pipe(ModuleFactory.VUAmultiwordtagger.getName(), EHUpos), pipelineFields, new RunModuleFunction(ModuleFactory.VUAmultiwordtagger), Fields.RESULTS);
		Pipe EHUnerc= new Each(new Pipe(ModuleFactory.EHUnerc.getName(), VUAmultiwordtagger), pipelineFields, new RunModuleFunction(ModuleFactory.EHUnerc), Fields.RESULTS);
		Pipe VUAopinionminer= new Each(new Pipe(ModuleFactory.VUAopinionminer.getName(), EHUnerc), pipelineFields, new RunModuleFunction(ModuleFactory.VUAopinionminer), Fields.RESULTS);
		Pipe VUAsvmwsd= new Each(new Pipe(ModuleFactory.VUAsvmwsd.getName(), VUAopinionminer), pipelineFields, new RunModuleFunction(ModuleFactory.VUAsvmwsd), Fields.RESULTS);
		Pipe EHUned= new Each(new Pipe(ModuleFactory.EHUned.getName(), VUAsvmwsd), pipelineFields, new RunModuleFunction(ModuleFactory.EHUned), Fields.RESULTS);
		Pipe EHUsrl= new Each(new Pipe(ModuleFactory.EHUsrl.getName(), EHUned), pipelineFields, new RunModuleFunction(ModuleFactory.EHUsrl), Fields.RESULTS);
		Pipe FBKtime= new Each(new Pipe(ModuleFactory.FBKtime.getName(), EHUsrl), pipelineFields, new RunModuleFunction(ModuleFactory.FBKtime), Fields.RESULTS);
		Pipe VUAeventcoref= new Each(new Pipe(ModuleFactory.VUAeventcoref.getName(), FBKtime), pipelineFields, new RunModuleFunction(ModuleFactory.VUAeventcoref), Fields.RESULTS);
		Pipe VUAfactuality= new Each(new Pipe(ModuleFactory.VUAfactuality.getName(), VUAeventcoref), pipelineFields, new RunModuleFunction(ModuleFactory.VUAfactuality), Fields.RESULTS);
		Checkpoint checkPoint = new Checkpoint("Checkpoint", VUAfactuality);
		
		Pipe succesDocs = new Each(new Pipe("Select succesful docs", checkPoint), pipelineFields, new FailedFilter());
		Pipe sstrip = new Each(new Pipe("Strip docFailed from succesful docs", succesDocs), pipelineFields, new StripField(), Fields.RESULTS);
		
		Pipe failedDocs = new Each(new Pipe("Select failed docs", checkPoint), pipelineFields, new SuccessFilter());
		Pipe fstrip = new Each(new Pipe("Strip docFailed from failed docs", failedDocs), pipelineFields, new StripField(), Fields.RESULTS);
		
		WritableSequenceFile outseq = new WritableSequenceFile(docFields, Text.class, Text.class);
		SequenceFile checkPointSeq = new SequenceFile(Fields.ALL);
		
		@SuppressWarnings("rawtypes")
		Tap successSink = new Hfs(outseq, outPath);
		@SuppressWarnings("rawtypes")
		Tap checkpointSink = new Hfs(checkPointSeq, outPath + "_checkpoint");
		@SuppressWarnings("rawtypes")
		Tap failedSink = new Hfs(outseq, errorPath);

		return FlowDef.flowDef().addSource(insertField, docTap).addCheckpoint(checkPoint, checkpointSink).addTailSink(sstrip, successSink).addTailSink(fstrip, failedSink);
	}

}
