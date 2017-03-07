/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.codegeneration;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import org.apache.flink.core.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link TemplateManager} is a singleton class that provides template rendering functionalities for code generation.
 * Such functionalities are caching, writing generated code to a file.
 */
public class TemplateManager {
	// ------------------------------------------------------------------------
	//                                   Constants
	// ------------------------------------------------------------------------
	public static final String RESOURCE_PATH  = getPathtoResources();
	public static final String TEMPLATE_PATH  = RESOURCE_PATH + "/templates";
	// TODO: generated this folder if it doesn't exist
	public static final String GENERATING_PATH      = createSubFolder("generatedcode");
	public static final String TEMPLATE_ENCODING    = "UTF-8";

	private static final Logger LOG = LoggerFactory.getLogger(TemplateManager.class);

	// ------------------------------------------------------------------------
	//                                   Singleton Attribute
	// ------------------------------------------------------------------------
	private static TemplateManager templateManager;

	// ------------------------------------------------------------------------
	//                                   Attributes
	// ------------------------------------------------------------------------
	private Configuration templateConf;
	private Map<String,Boolean> generatedSorter;

	/**
	 * Constructor
	 * @throws IOException
	 */
	public TemplateManager() throws IOException {
		templateConf = new Configuration();
		templateConf.setDirectoryForTemplateLoading(new File(TEMPLATE_PATH));
		templateConf.setDefaultEncoding(TEMPLATE_ENCODING);
		templateConf.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
		generatedSorter = new HashMap<>();
	}


	/**
	 * A method to get a singleton instance
	 * or create one if it has been created yet
	 * @return
	 * @throws IOException
	 */
	public static TemplateManager getInstance() throws IOException {
		if( templateManager == null ) {
			synchronized (TemplateManager.class){
				templateManager = new TemplateManager();
			}
		}

		return templateManager;
	}


	/**
	 * Render sorter template with generated code provided by SorterTemplateModel and write the content to a file
	 * and cache the result for later calls
	 * @param SorterTemplateModel object
	 * @return name of the generated sorter
	 * @throws IOException
	 * @throws TemplateException
	 */
	public String getGeneratedCode(SorterTemplateModel model) throws IOException, TemplateException {
		Template template = templateConf.getTemplate(model.TEMPLATE_NAME);

		String generatedFilename = model.getSorterName();

		if( generatedSorter.getOrDefault(generatedFilename, false) ){
			if (LOG.isDebugEnabled()) {
				LOG.debug("Served from cache : "+generatedFilename);
			}
			return generatedFilename;
		}

		FileOutputStream fs = new FileOutputStream(new File(GENERATING_PATH +"/"+generatedFilename+".java"));

		Writer output = new OutputStreamWriter(fs);
		Map templateVariables = model.getTemplateVariables();
		template.process(templateVariables, output);

		fs.close();
		output.close();

		generatedSorter.put(generatedFilename, true);

		return generatedFilename;
	}


	/**
	 * Get the Path of the Working Folder where the resources directory is
	 * @retun the absolute path as String
	 */
	private static String getPathtoResources() {
		FileSystem fs = FileSystem.getLocalFileSystem();
		return fs.getWorkingDirectory().toUri().getPath() + "/flink-runtime/resources";
	}

	/**
	 * Create a subfolder in the Temp Directory
	 * @param nameSubfolder: the name of the new folder
	 * @return path of the new */
	private static String createSubFolder (String nameSubfolder){
		try {
			File tempDirectory;
			tempDirectory = File.createTempFile("codegeneration", Long.toString(System.nanoTime()));
			tempDirectory.delete();
			tempDirectory.mkdir();
			tempDirectory = new File(tempDirectory.getAbsolutePath() + "/" + nameSubfolder);
			tempDirectory.delete();
			tempDirectory.mkdir();
			return tempDirectory.getAbsolutePath();
		} catch (IOException e){
			return "";
		}
	}
}
