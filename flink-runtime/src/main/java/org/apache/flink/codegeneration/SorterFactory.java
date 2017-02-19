package org.apache.flink.codegeneration;

import freemarker.template.TemplateException;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.NormalizedKeySorter;
import org.codehaus.janino.JavaSourceClassLoader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by heytitle on 2/19/17.
 */
public class SorterFactory {
	/* Set up janino */
	private static final ClassLoader cl = new JavaSourceClassLoader(
		SorterFactory.class.getClassLoader(),
		new File[] { new File(TemplateManager.GENERATING_PATH) }, // optionalSourcePath
		(String) null                     // optionalCharacterEncoding
	);

	public static NormalizedKeySorter createSorter(TypeSerializer serializer, TypeComparator comparator, List<MemorySegment> memory ) throws IOException, TemplateException, ClassNotFoundException, IllegalAccessException, InstantiationException {
		String className = TemplateManager.getGeneratedCode(new SorterTemplateModel());

		Object myClass = cl.loadClass(className).newInstance();

		System.out.println(">> " + myClass.toString());

		return new NormalizedKeySorter(serializer, comparator, memory);
	}
}
