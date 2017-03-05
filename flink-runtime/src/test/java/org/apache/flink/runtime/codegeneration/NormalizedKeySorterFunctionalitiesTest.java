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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.ValueMode;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Random;


public class NormalizedKeySorterFunctionalitiesTest {
	
	private static final long SEED = 649180756312423613L;
	
	private static final long SEED2 = 97652436586326573L;

	private static final int KEY_MAX = Integer.MAX_VALUE;

	private static final int VALUE_LENGTH = 118;

	private static final int MEMORY_SIZE = 1024 * 1024 * 64;
	
	private static final int MEMORY_PAGE_SIZE = 32 * 1024; 

	private MemoryManager memoryManager;
	private SorterFactory sorterFactory;

	private ExecutionConfig executionConfig = new ExecutionConfig(){
		{
			setCodeGenerationForSorterEnabled(true);
		}
	};


	@Before
	public void beforeTest() throws IOException {
		this.memoryManager = new MemoryManager(MEMORY_SIZE, 1, MEMORY_PAGE_SIZE, MemoryType.HEAP, true);
		this.sorterFactory = SorterFactory.getInstance();
	}

	@After
	public void afterTest() {
		if (!this.memoryManager.verifyEmpty()) {
			Assert.fail("Memory Leak: Some memory has not been returned to the memory manager.");
		}
		
		if (this.memoryManager != null) {
			this.memoryManager.shutdown();
			this.memoryManager = null;
		}
	}

	private InMemorySorter<Tuple2<Integer, String>> newSortBuffer(List<MemorySegment> memory) throws Exception {
		return this.sorterFactory.createSorter(executionConfig, TestData.getIntStringTupleSerializer(), TestData.getIntStringTupleComparator(), memory);
	}

	@Test
	public void testWriteAndRead() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);

		InMemorySorter<Tuple2<Integer, String>> sorter = newSortBuffer(memory);
		TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
			ValueMode.RANDOM_LENGTH);

		// write the records
		Tuple2<Integer, String> record = new Tuple2<>();
		int num = -1;
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record));

		// re-read the records
		generator.reset();
		Tuple2<Integer, String> readTarget = new Tuple2<>();

		int i = 0;
		while (i < num) {
			generator.next(record);
			readTarget = sorter.getRecord(readTarget, i++);

			int rk = readTarget.f0;
			int gk = record.f0;

			String rv = readTarget.f1;
			String gv = record.f1;

			Assert.assertEquals("The re-read key is wrong", gk, rk);
			Assert.assertEquals("The re-read value is wrong", gv, rv);
		}

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}
//
	@Test
	public void testWriteAndIterator() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);

		InMemorySorter<Tuple2<Integer, String>> sorter = newSortBuffer(memory);
		TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
			ValueMode.RANDOM_LENGTH);

		// write the records
		Tuple2<Integer, String> record = new Tuple2<>();
		do {
			generator.next(record);
		}
		while (sorter.write(record));

		// re-read the records
		generator.reset();
		MutableObjectIterator<Tuple2<Integer, String>> iter = sorter.getIterator();
		Tuple2<Integer, String> readTarget = new Tuple2<>();

		while ((readTarget = iter.next(readTarget)) != null) {
			generator.next(record);

			int rk = readTarget.f0;
			int gk = record.f0;

			String rv = readTarget.f1;
			String gv = record.f1;

			Assert.assertEquals("The re-read key is wrong", gk, rk);
			Assert.assertEquals("The re-read value is wrong", gv, rv);
		}

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testReset() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);

		InMemorySorter<Tuple2<Integer, String>> sorter = newSortBuffer(memory);
		TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.FIX_LENGTH);

		// write the buffer full with the first set of records
		Tuple2<Integer, String> record = new Tuple2<>();
		int num = -1;
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record));

		sorter.reset();

		// write a second sequence of records. since the values are of fixed length, we must be able to write an equal number
		generator = new TestData.TupleGenerator(SEED2, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM, ValueMode.FIX_LENGTH);

		// write the buffer full with the first set of records
		int num2 = -1;
		do {
			generator.next(record);
			num2++;
		}
		while (sorter.write(record));

		Assert.assertEquals("The number of records written after the reset was not the same as before.", num, num2);

		// re-read the records
		generator.reset();
		Tuple2<Integer, String> readTarget = new Tuple2<>();

		int i = 0;
		while (i < num) {
			generator.next(record);
			readTarget = sorter.getRecord(readTarget, i++);

			int rk = readTarget.f0;
			int gk = record.f0;

			String rv = readTarget.f1;
			String gv = record.f1;

			Assert.assertEquals("The re-read key is wrong", gk, rk);
			Assert.assertEquals("The re-read value is wrong", gv, rv);
		}

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	/**
	 * The swap test fills the sort buffer and swaps all elements such that they are
	 * backwards. It then resets the generator, goes backwards through the buffer
	 * and compares for equality.
	 */
	@Test
	public void testSwap() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);

		InMemorySorter<Tuple2<Integer, String>> sorter = newSortBuffer(memory);
		TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
			ValueMode.RANDOM_LENGTH);

		// write the records
		Tuple2<Integer, String> record = new Tuple2<>();
		int num = -1;
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record));

		// swap the records
		int start = 0, end = num - 1;
		while (start < end) {
			sorter.swap(start++, end--);
		}

		// re-read the records
		generator.reset();
		Tuple2<Integer, String> readTarget = new Tuple2<>();

		int i = num - 1;
		while (i >= 0) {
			generator.next(record);
			readTarget = sorter.getRecord(readTarget, i--);

			int rk = readTarget.f0;
			int gk = record.f0;

			String rv = readTarget.f1;
			String gv = record.f1;

			Assert.assertEquals("The re-read key is wrong", gk, rk);
			Assert.assertEquals("The re-read value is wrong", gv, rv);
		}

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	/**
	 * The compare test creates a sorted stream, writes it to the buffer and
	 * compares random elements. It expects that earlier elements are lower than later
	 * ones.
	 */
	@Test
	public void testCompare() throws Exception {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);

		InMemorySorter<Tuple2<Integer, String>> sorter = newSortBuffer(memory);
		TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.SORTED,
			ValueMode.RANDOM_LENGTH);

		// write the records
		Tuple2<Integer, String> record = new Tuple2<>();
		int num = -1;
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record));

		// compare random elements
		Random rnd = new Random(SEED << 1);
		for (int i = 0; i < 2 * num; i++) {
			int pos1 = rnd.nextInt(num);
			int pos2 = rnd.nextInt(num);

			int cmp = sorter.compare(pos1, pos2);

			if (pos1 < pos2) {
				Assert.assertTrue(cmp <= 0);
			}
			else {
				Assert.assertTrue(cmp >= 0);
			}
		}

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}
}
