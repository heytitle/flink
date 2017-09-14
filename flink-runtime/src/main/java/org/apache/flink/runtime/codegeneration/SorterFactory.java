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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.operators.sort.FixedLengthRecordSorter;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.NormalizedKeySorter;

import freemarker.template.TemplateException;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;

/**
 * {@link SorterFactory} is a singleton class that provides functionalities to create the most suitable sorter
 * for underlying data based on {@link TypeComparator}.
 * Note: the generated code can be inspected by configuring Janino to write the code that is being compiled
 * to a file, see http://janino-compiler.github.io/janino/#debugging
 */
public class SorterFactory {
	// ------------------------------------------------------------------------
	//                                   Constants
	// ------------------------------------------------------------------------
	private static final Logger LOG = LoggerFactory.getLogger(SorterFactory.class);

	/** Fixed length records with a length below this threshold will be in-place sorted, if possible. */
	private static final int THRESHOLD_FOR_IN_PLACE_SORTING = 32;

	// ------------------------------------------------------------------------
	//                                   Singleton Attribute
	// ------------------------------------------------------------------------
	private static SorterFactory sorterFactory;

	// ------------------------------------------------------------------------
	//                                   Attributes
	// ------------------------------------------------------------------------
	private SimpleCompiler classCompiler;
	private TemplateManager templateManager;
	private HashMap<String, Constructor> constructorCache;

	/**
	 * Constructor.
	 * @throws IOException
	 */
	private SorterFactory() throws IOException {
		this.templateManager = TemplateManager.getInstance();
		this.classCompiler = new SimpleCompiler();
		this.constructorCache = new HashMap<>();
	}

	/**
	 * A method to get a singleton instance
	 * or create one if it hasn't been created yet.
	 * @return
	 * @throws IOException
	 */
	public static synchronized SorterFactory getInstance() throws IOException {
		if (sorterFactory == null){
			sorterFactory = new SorterFactory();
		}

		return sorterFactory;
	}


	/**
	 * Create a sorter for the given type comparator and
	 * assign serializer, comparator and memory to the sorter.
	 * @param serializer
	 * @param comparator
	 * @param memory
	 * @return
	 * @throws IOException
	 * @throws TemplateException
	 * @throws ClassNotFoundException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException
	 */
	@SuppressWarnings("unchecked")
	public <T> InMemorySorter<T> createSorter(ExecutionConfig config, TypeSerializer<T> serializer, TypeComparator<T> comparator, List<MemorySegment> memory) throws IOException, TemplateException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException, CompileException {

		InMemorySorter<T> sorter;

		if (config.isCodeGenerationForSorterEnabled()){
			SorterTemplateModel sorterModel = new SorterTemplateModel(comparator);

			Constructor sorterConstructor;

			synchronized (this){
				if (constructorCache.getOrDefault(sorterModel.getSorterName(), null) != null) {
					sorterConstructor = constructorCache.get(sorterModel.getSorterName());
				} else {
					String sorterName = sorterModel.getSorterName();
					String generatedCode = this.templateManager.getGeneratedCode(sorterModel);
					this.classCompiler.cook(generatedCode);

					sorterConstructor = this.classCompiler.getClassLoader().loadClass(sorterName).getConstructor(
						TypeSerializer.class, TypeComparator.class, List.class
					);

					constructorCache.put(sorterName, sorterConstructor);
				}
			}

			sorter = (InMemorySorter<T>) sorterConstructor.newInstance(serializer, comparator, memory);

			if (LOG.isInfoEnabled()){
				LOG.info("Using a custom sorter : " + sorter.toString());
			}
		} else {
			// instantiate a fix-length in-place sorter, if possible, otherwise the out-of-place sorter
			if (comparator.supportsSerializationWithKeyNormalization() &&
					serializer.getLength() > 0 && serializer.getLength() <= THRESHOLD_FOR_IN_PLACE_SORTING &&
					comparator.isNormalizedKeyPrefixOnly(comparator.getNormalizeKeyLen())) {
				// Note about the last part of the condition:
				// FixedLengthRecordSorter doesn't do an additional check after the bytewise comparison, so
				// we cannot choose that if the normalized key doesn't always determine the order.
				// (cf. the part of NormalizedKeySorter.compare after the if)
				sorter = new FixedLengthRecordSorter<>(serializer, comparator.duplicate(), memory);
			} else {
				sorter = new NormalizedKeySorter<>(serializer, comparator.duplicate(), memory);
			}
		}

		return sorter;
	}
}