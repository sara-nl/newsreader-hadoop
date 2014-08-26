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
package nl.surfsara.newsreader.pipeline.modules;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

public class FBKTime extends SubprocessModule {
	private static final Logger logger = Logger.getLogger(FBKTime.class);

	private ModuleFactory mfi = ModuleFactory.FBKtime;

	public FBKTime(ModuleFactory modulefactory) {
		// TODO this smells.. Necessary for the reflection..
		// Ignore argument
	}
	
	@Override
	public Module call() throws Exception {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ByteArrayOutputStream bes = new ByteArrayOutputStream();

		String filename = UUID.randomUUID().toString();
		File xmlf = new File(getLocalDirectory() + "/" + filename);
		FileOutputStream fos = new FileOutputStream(xmlf);
		InputStream xmlStream = IOUtils.toInputStream(getInputDocument(), "UTF-8");
		IOUtils.copy(xmlStream, fos);
		fos.flush();
		fos.close();
		xmlStream.close();
		
		File f = new File(mfi.getModulePath() + "/run.sh");
		File component = new File(mfi.getModulePath());
		File scratch = new File(getLocalDirectory());

		super.setCommandLine("/bin/bash " + f.getAbsolutePath() + " " + component.getAbsolutePath() + " " + scratch.getAbsolutePath() + " " + xmlf.getAbsolutePath());

		super.setSubProcessStdIn(IOUtils.toInputStream(getInputDocument()));
		super.setSubProcessStdOut(bos);
		super.setSubProcessStdErr(bes);
		super.runSubprocess();

		String stderr = bes.toString();
		int newlines = stderr.split(System.getProperty("line.separator")).length;
		if (newlines > mfi.getNumErrorLines()) {
			setFailed(true);
		}
		logger.error(stderr);
		bos.flush();
		setOutputDocument(bos.toString());
		bos.close();
		bes.close();
		xmlf.delete();
		return this;
	}
}
