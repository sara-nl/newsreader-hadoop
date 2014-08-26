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

import java.lang.reflect.Constructor;

/**
 * Enumeration that defines (and doubles as factory pattern) for each Newsreader
 * component its name, executing class, timeout and allowable newlines in
 * stderr.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public enum ModuleFactory {
	EHUtok("EHU-tok", GenericNewsreaderModule.class, 600000, 4), EHUpos("EHU-pos", GenericNewsreaderModule.class, 600000, 4), VUAmultiwordtagger("VUA-multiwordtagger", GenericNewsreaderModule.class, 600000, 4), EHUnerc("EHU-nerc", GenericNewsreaderModule.class, 600000, 4), VUAopinionminer("VUA-opinion-miner", GenericNewsreaderModule.class, 600000, 4), VUAsvmwsd("VUA-svm-wsd", GenericNewsreaderModule.class, 600000, 4), EHUned("EHU-ned", GenericNewsreaderModule.class, 600000, 10), EHUsrl("EHU-srl", GenericNewsreaderModule.class, 600000, 4), FBKtime("FBK-time", FBKTime.class, 600000, 4), VUAeventcoref("VUA-eventcoref", GenericNewsreaderModule.class, 600000, 4), VUAfactuality("VUA-factuality", GenericNewsreaderModule.class, 600000, 4);

	private final String name;
	private final Class<? extends Module> c;
	private long timeout;
	private int numErrorLines;

	private ModuleFactory(String name, Class<? extends Module> c, long timeout, int numErrorLines) {
		this.name = name;
		this.c = c;
		this.timeout = timeout;
		this.numErrorLines = numErrorLines;
	}

	public Module getInstance() throws Exception {
		Constructor<? extends Module> constructor = c.getConstructor(ModuleFactory.class);
		return constructor.newInstance(this);
	}

	public String getName() {
		return name;
	}

	public String getModulePath() {
		return ModuleConstants.ARCHIVEROOT + "/" + getName();
	}

	public int getNumErrorLines() {
		return numErrorLines;
	}

	public long getTimeout() {
		return timeout;
	}

}
