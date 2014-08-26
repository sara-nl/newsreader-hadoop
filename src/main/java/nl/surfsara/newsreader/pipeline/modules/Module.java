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

import java.util.concurrent.Callable;

public interface Module extends Callable<Module> {
	public abstract void setInputDocument(String xmlText);

	public abstract String getInputDocument();

	public abstract void setOutputDocument(String xmlText);
	
	public abstract String getOutputDocument();

	public abstract boolean hasFailed();

	public abstract void setFailed(boolean failed);

	public abstract void setLocalDirectory(String localDir);

	public abstract String getLocalDirectory();
}
