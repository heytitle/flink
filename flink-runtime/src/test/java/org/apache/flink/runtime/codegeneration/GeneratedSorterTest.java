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

import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.twitter.chill.Tuple2Serializer;
import freemarker.template.TemplateException;
import org.apache.commons.math.random.RandomGenerator;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.TupleComparator;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.runtime.codegeneration.SorterFactory;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.InMemorySorter;
import org.apache.flink.runtime.operators.sort.NormalizedKeySorter;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.testutils.TestData;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.KeyMode;
import org.apache.flink.runtime.operators.testutils.TestData.TupleGenerator.ValueMode;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Random;


public class GeneratedSorterTest {
	
	private static final long SEED = 649180756312423613L;
	
	private static final long SEED2 = 97652436586326573L;

	private static final int KEY_MAX = Integer.MAX_VALUE;

	private static final int VALUE_LENGTH = 118;

	private static final int MEMORY_SIZE = 1024 * 1024 * 64;
	
	private static final int MEMORY_PAGE_SIZE = 32 * 1024; 

	private MemoryManager memoryManager;


	@Before
	public void beforeTest() {
		this.memoryManager = new MemoryManager(MEMORY_SIZE, 1, MEMORY_PAGE_SIZE, MemoryType.HEAP, true);
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
		return SorterFactory.createSorter(TestData.getIntStringTupleSerializer(), TestData.getIntStringTupleComparator(), memory);
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

	@Test
	public void testSort() throws Exception {
		final int NUM_RECORDS = 559273;

		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);

		InMemorySorter<Tuple2<Integer, String>> sorter = newSortBuffer(memory);
		TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, VALUE_LENGTH, KeyMode.RANDOM,
			ValueMode.RANDOM_LENGTH);

		// write the records
		Tuple2<Integer, String> record = new Tuple2<>();
		int num = 0;
		do {
			generator.next(record);
			num++;
		}
		while (sorter.write(record) && num < NUM_RECORDS);

		QuickSort qs = new QuickSort();
		qs.sort(sorter);

		MutableObjectIterator<Tuple2<Integer, String>> iter = sorter.getIterator();
		Tuple2<Integer, String> readTarget = new Tuple2<>();

		iter.next(readTarget);
		int last = readTarget.f0;

		while ((readTarget = iter.next(readTarget)) != null) {
			int current = readTarget.f0;

			final int cmp = last - current;
			if (cmp > 0) {
				Assert.fail("Next key is not larger or equal to previous key.");
			}

			last = current;
		}

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testSortShortStringKeys() throws Exception {

		@SuppressWarnings("unchecked")
		List<MemorySegment> memory = createMemory();
		TypeComparator<Tuple2<Integer, String>> accessors = TestData.getIntStringTupleTypeInfo().createComparator(new int[]{1}, new boolean[]{true}, 0, null);
		InMemorySorter<Tuple2<Integer, String>> sorter = createSorter(TestData.getIntStringTupleSerializer(), accessors, memory);

		TestData.TupleGenerator generator = new TestData.TupleGenerator(SEED, KEY_MAX, 5, KeyMode.RANDOM,
			ValueMode.FIX_LENGTH);

		Tuple2<Integer, String> record = new Tuple2<>();
		do {
			generator.next(record);
		}
		while (sorter.write(record));

		QuickSort qs = new QuickSort();
		qs.sort(sorter);

		MutableObjectIterator<Tuple2<Integer, String>> iter = sorter.getIterator();
		Tuple2<Integer, String> readTarget = new Tuple2<>();

		iter.next(readTarget);
		String last = readTarget.f1;

		while ((readTarget = iter.next(readTarget)) != null) {
			String current = readTarget.f1;

			final int cmp = last.compareTo(current);
			if (cmp > 0) {
				Assert.fail("Next value is not larger or equal to previous value.");
			}

			last = current;
		}

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testSortLongTypeKeys() throws Exception {

		@SuppressWarnings("unchecked")
		List<MemorySegment> memory = createMemory();

		TypeSerializer[] insideSerializers = {
			LongSerializer.INSTANCE, IntSerializer.INSTANCE
		};

		TupleSerializer<Tuple2<Long,Integer>> serializer = new TupleSerializer<Tuple2<Long, Integer>>(
			(Class<Tuple2<Long, Integer>>) (Class<?>) Tuple2.class, insideSerializers
		);

		TupleComparator<Tuple2<Long, Integer>> comparators = new TupleComparator<Tuple2<Long, Integer>>(
			new int[]{0}, new TypeComparator[]{ new LongComparator(true) }, insideSerializers
		);

		InMemorySorter<Tuple2<Long, Integer>> sorter = createSorter( serializer, comparators, memory);

		Random randomGenerator = new Random(SEED);

		Tuple2<Long, Integer> record = new Tuple2<>();
		do {
			record.setFields(randomGenerator.nextLong(), randomGenerator.nextInt());
		}
		while (sorter.write(record));

		QuickSort qs = new QuickSort();
		qs.sort(sorter);

		MutableObjectIterator<Tuple2<Long, Integer>> iter = sorter.getIterator();
		Tuple2<Long, Integer> readTarget = new Tuple2<>();

		iter.next(readTarget);
		Long last = readTarget.f0;

		while ((readTarget = iter.next(readTarget)) != null) {
			Long current = readTarget.f0;

			final int cmp = last.compareTo(current);
			if (cmp > 0) {
				Assert.fail("Next value is not larger or equal to previous value.");
			}

			last = current;
		}

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	@Test
	public void testSort12BytesKeys() throws Exception {
		// Tuple( (Long,Int), Int )

		@SuppressWarnings("unchecked")
		List<MemorySegment> memory = createMemory();

		TypeSerializer keySerializer = new TupleSerializer(Tuple2.class, new TypeSerializer[]{
			LongSerializer.INSTANCE,
			IntSerializer.INSTANCE
		});

		TypeComparator keyComparator = new TupleComparator(
			new int[]{0,1},
			new TypeComparator[]{
				new LongComparator(true),
				new IntComparator(true)
			},
			new TypeSerializer[] { keySerializer }
		);

		TypeSerializer[] insideSerializers = {
			keySerializer,
			IntSerializer.INSTANCE
		};

		TupleSerializer<Tuple2<Tuple2<Long,Integer>,Integer>> serializer = new TupleSerializer<>(
			(Class<Tuple2<Tuple2<Long,Integer>, Integer>>) (Class<?>) Tuple2.class, insideSerializers
		);

		TupleComparator<Tuple2<Tuple2<Long,Integer>, Integer>> comparators = new TupleComparator<>(
			new int[]{0}, new TypeComparator[]{ keyComparator },
			insideSerializers
		);

		InMemorySorter<Tuple2<Tuple2<Long, Integer>, Integer>> sorter = createSorter( serializer, comparators, memory);
//		InMemorySorter<Tuple2<Tuple2<Long, Integer>, Integer>> sorter = new NormalizedKeySorter<>(serializer, comparators, memory);

		Random randomGenerator = new Random(SEED);

		Tuple2<Tuple2<Long, Integer>, Integer> record = new Tuple2<>();
		do {

			Tuple2<Long,Integer> insideTp = new Tuple2<>();
			insideTp.setFields(2L, randomGenerator.nextInt());
			record.setFields( insideTp, randomGenerator.nextInt());
		}
		while (sorter.write(record));

		QuickSort qs = new QuickSort();
		qs.sort(sorter);

		MutableObjectIterator<Tuple2<Tuple2<Long, Integer>, Integer>> iter = sorter.getIterator();
		Tuple2<Tuple2<Long, Integer>, Integer> readTarget = iter.next();

		Tuple2<Long,Integer> last = readTarget.f0;

		while ((readTarget = iter.next()) != null) {
			Tuple2<Long,Integer> current = readTarget.f0;

			final int cmp = keyComparator.compare(last, current);
			if (cmp > 0) {
				Assert.fail("Next value is not larger or equal to previous value.");
			}

			last = current;
		}

		// release the memory occupied by the buffers
		sorter.dispose();
		this.memoryManager.release(memory);
	}

	private InMemorySorter createSorter(TypeSerializer serializer, TypeComparator comparator, List<MemorySegment> memory ) throws IllegalAccessException, TemplateException, IOException, InstantiationException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
		return SorterFactory.createSorter(
			serializer,
			comparator,
			memory
		);
	}

	private List<MemorySegment> createMemory() throws MemoryAllocationException {
		final int numSegments = MEMORY_SIZE / MEMORY_PAGE_SIZE;
		final List<MemorySegment> memory = this.memoryManager.allocatePages(new DummyInvokable(), numSegments);

		return memory;
	}

}
