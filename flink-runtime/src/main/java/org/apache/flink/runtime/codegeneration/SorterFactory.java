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

import freemarker.template.TemplateException;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.codehaus.janino.JavaSourceClassLoader;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

// TODO: Better way to make SorterFactory a singleton class
public class SorterFactory {

	// Setup Janino
	// TODO: find a better way to instantince Janion in this class
	private static final ClassLoader classLoader = new JavaSourceClassLoader(
		SorterFactory.class.getClassLoader(),
			new File[] { new File(TemplateManager.GENERATING_PATH) },
		"UTF-8"
	);

	public static InMemorySorter createSorter(TypeSerializer serializer, TypeComparator comparator, List<MemorySegment> memory ) throws IOException, TemplateException, ClassNotFoundException, IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {

		String className = TemplateManager.getGeneratedCode(new SorterTemplateModel(comparator));

		Constructor sorterConstructor = classLoader.loadClass(className).getConstructor(
			TypeSerializer.class, TypeComparator.class, List.class
		);

		Object generatedSorter = sorterConstructor.newInstance(serializer, comparator, memory);

		System.out.println(">> " + generatedSorter.toString());

		return (InMemorySorter)generatedSorter;
	}
}
