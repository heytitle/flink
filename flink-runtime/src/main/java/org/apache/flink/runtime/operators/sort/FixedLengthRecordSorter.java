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


package org.apache.flink.runtime.operators.sort;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.ChannelWriterOutputView;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.util.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 
 */
public final class FixedLengthRecordSorter<T> implements InMemorySorter<T> {
	
	private static final int MIN_REQUIRED_BUFFERS = 3;

	// ------------------------------------------------------------------------
	//                               Members
	// ------------------------------------------------------------------------

	private final byte[] swapBuffer;
	
	private final TypeSerializer<T> serializer;
	
	private final TypeComparator<T> comparator;
	
	private final SingleSegmentOutputView outView;
	
	private final SingleSegmentInputView inView;
	
	private MemorySegment currentSortBufferSegment;
	
	private int currentSortBufferOffset;
	
	private final ArrayList<MemorySegment> freeMemory;
	
	private final ArrayList<MemorySegment> sortBuffer;
	
	private long sortBufferBytes;
	
	private int numRecords;
	
	private final int numKeyBytes;
	
	private final int recordSize;
	
	private final int recordsPerSegment;
	
	private final int lastEntryOffset;
	
	private final int segmentSize;
	
	private final int totalNumBuffers;
	
	private final boolean useNormKeyUninverted;
	
	private final T recordInstance;

	private static final IndexedSorter alt = new HeapSort();

	// -------------------------------------------------------------------------
	// Constructors / Destructors
	// -------------------------------------------------------------------------
	
	public FixedLengthRecordSorter(TypeSerializer<T> serializer, TypeComparator<T> comparator, 
			List<MemorySegment> memory)
	{
		if (serializer == null || comparator == null || memory == null) {
			throw new NullPointerException();
		}
		
		this.serializer = serializer;
		this.comparator = comparator;
		this.useNormKeyUninverted = !comparator.invertNormalizedKey();
		
		// check the size of the first buffer and record it. all further buffers must have the same size.
		// the size must also be a power of 2
		this.totalNumBuffers = memory.size();
		if (this.totalNumBuffers < MIN_REQUIRED_BUFFERS) {
			throw new IllegalArgumentException("Normalized-Key sorter requires at least " + MIN_REQUIRED_BUFFERS + " memory buffers.");
		}
		this.segmentSize = memory.get(0).size();
		this.recordSize = serializer.getLength();
		this.numKeyBytes = this.comparator.getNormalizeKeyLen();
		
		// check that the serializer and comparator allow our operations
		if (this.recordSize <= 0) {
			throw new IllegalArgumentException("This sorter works only for fixed-length data types.");
		} else if (this.recordSize > this.segmentSize) {
			throw new IllegalArgumentException("This sorter works only for record lengths below the memory segment size.");
		} else if (!comparator.supportsSerializationWithKeyNormalization()) {
			throw new IllegalArgumentException("This sorter requires a comparator that supports serialization with key normalization.");
		}
		
		// compute the entry size and limits
		this.recordsPerSegment = segmentSize / this.recordSize;
		this.lastEntryOffset = (this.recordsPerSegment - 1) * this.recordSize;
		this.swapBuffer = new byte[this.recordSize];
		
		this.freeMemory = new ArrayList<MemorySegment>(memory);
		
		// create the buffer collections
		this.sortBuffer = new ArrayList<MemorySegment>(16);
		this.outView = new SingleSegmentOutputView(this.segmentSize);
		this.inView = new SingleSegmentInputView(this.lastEntryOffset + this.recordSize);
		this.currentSortBufferSegment = nextMemorySegment();
		this.sortBuffer.add(this.currentSortBufferSegment);
		this.outView.set(this.currentSortBufferSegment);
		
		this.recordInstance = this.serializer.createInstance();
	}

	@Override
	public int recordSize() {
		return recordSize;
	}

	@Override
	public int recordsPerSegment() {
		return recordsPerSegment;
	}


	// -------------------------------------------------------------------------
	// Memory Segment
	// -------------------------------------------------------------------------

	/**
	 * Resets the sort buffer back to the state where it is empty. All contained data is discarded.
	 */
	@Override
	public void reset() {
		// reset all offsets
		this.numRecords = 0;
		this.currentSortBufferOffset = 0;
		this.sortBufferBytes = 0;
		
		// return all memory
		this.freeMemory.addAll(this.sortBuffer);
		this.sortBuffer.clear();
		
		// grab first buffers
		this.currentSortBufferSegment = nextMemorySegment();
		this.sortBuffer.add(this.currentSortBufferSegment);
		this.outView.set(this.currentSortBufferSegment);
	}

	/**
	 * Checks whether the buffer is empty.
	 * 
	 * @return True, if no record is contained, false otherwise.
	 */
	@Override
	public boolean isEmpty() {
		return this.numRecords == 0;
	}
	
	@Override
	public void dispose() {
		this.freeMemory.clear();
		this.sortBuffer.clear();
	}
	
	@Override
	public long getCapacity() {
		return ((long) this.totalNumBuffers) * this.segmentSize;
	}
	
	@Override
	public long getOccupancy() {
		return this.sortBufferBytes;
	}

	// -------------------------------------------------------------------------
	// Retrieving and Writing
	// -------------------------------------------------------------------------
	
	@Override
	public T getRecord(int logicalPosition) throws IOException {
		return getRecord(serializer.createInstance(), logicalPosition);
	}
	
	@Override
	public T getRecord(T reuse, int logicalPosition) throws IOException {
		final int buffer = logicalPosition / this.recordsPerSegment;
		final int inBuffer = (logicalPosition % this.recordsPerSegment) * this.recordSize;
		this.inView.set(this.sortBuffer.get(buffer), inBuffer);
		return this.comparator.readWithKeyDenormalization(reuse, this.inView);
	}

	/**
	 * Writes a given record to this sort buffer. The written record will be appended and take
	 * the last logical position.
	 * 
	 * @param record The record to be written.
	 * @return True, if the record was successfully written, false, if the sort buffer was full.
	 * @throws IOException Thrown, if an error occurred while serializing the record into the buffers.
	 */
	@Override
	public boolean write(T record) throws IOException {
		// check whether we need a new memory segment for the sort index
		if (this.currentSortBufferOffset > this.lastEntryOffset) {
			if (memoryAvailable()) {
				this.currentSortBufferSegment = nextMemorySegment();
				this.sortBuffer.add(this.currentSortBufferSegment);
				this.outView.set(this.currentSortBufferSegment);
				this.currentSortBufferOffset = 0;
				this.sortBufferBytes += this.segmentSize;
			}
			else {
				return false;
			}
		}
		
		// serialize the record into the data buffers
		try {
			this.comparator.writeWithKeyNormalization(record, this.outView);
			this.numRecords++;
			this.currentSortBufferOffset += this.recordSize;
			return true;
		} catch (EOFException eofex) {
			throw new IOException("Error: Serialization consumes more bytes than announced by the serializer.");
		}
	}
	
	// ------------------------------------------------------------------------
	//                           Access Utilities
	// ------------------------------------------------------------------------
	
	private boolean memoryAvailable() {
		return !this.freeMemory.isEmpty();
	}
	
	private MemorySegment nextMemorySegment() {
		return this.freeMemory.remove(this.freeMemory.size() - 1);
	}

	// -------------------------------------------------------------------------
	// Sorting
	// -------------------------------------------------------------------------

	@Override
	public int compare(int i, int j) {
		final int bufferNumI = i / this.recordsPerSegment;
		final int segmentOffsetI = (i % this.recordsPerSegment) * this.recordSize;
		
		final int bufferNumJ = j / this.recordsPerSegment;
		final int segmentOffsetJ = (j % this.recordsPerSegment) * this.recordSize;
		
		final MemorySegment segI = this.sortBuffer.get(bufferNumI);
		final MemorySegment segJ = this.sortBuffer.get(bufferNumJ);
		
		int val = segI.compare(segJ, segmentOffsetI, segmentOffsetJ, this.numKeyBytes);
		return this.useNormKeyUninverted ? val : -val;
	}

	@Override
	public int compare(int iBufferNumber, int iBufferOffset, int jBufferNumber, int jBufferOffset) {
		final MemorySegment segI = this.sortBuffer.get(iBufferNumber);
		final MemorySegment segJ = this.sortBuffer.get(jBufferNumber);

		int val = segI.compare(segJ, iBufferOffset, jBufferOffset, this.numKeyBytes);
		return this.useNormKeyUninverted ? val : -val;
	}

	@Override
	public void swap(int i, int j) {
		final int bufferNumI = i / this.recordsPerSegment;
		final int segmentOffsetI = (i % this.recordsPerSegment) * this.recordSize;
		
		final int bufferNumJ = j / this.recordsPerSegment;
		final int segmentOffsetJ = (j % this.recordsPerSegment) * this.recordSize;
		
		final MemorySegment segI = this.sortBuffer.get(bufferNumI);
		final MemorySegment segJ = this.sortBuffer.get(bufferNumJ);
		
		segI.swapBytes(this.swapBuffer, segJ, segmentOffsetI, segmentOffsetJ, this.recordSize);
	}

	@Override
	public void swap(int iBufferNumber, int iBufferOffset, int jBufferNumber, int jBufferOffset) {
		final MemorySegment segI = this.sortBuffer.get(iBufferNumber);
		final MemorySegment segJ = this.sortBuffer.get(jBufferNumber);

		segI.swapBytes(this.swapBuffer, segJ, iBufferOffset, jBufferOffset, this.recordSize);
	}

	@Override
	public int size() {
		return this.numRecords;
	}

	// -------------------------------------------------------------------------
	
	/**
	 * Gets an iterator over all records in this buffer in their logical order.
	 * 
	 * @return An iterator returning the records in their logical order.
	 */
	@Override
	public final MutableObjectIterator<T> getIterator() {
		final SingleSegmentInputView startIn = new SingleSegmentInputView(this.recordsPerSegment * this.recordSize);
		startIn.set(this.sortBuffer.get(0), 0);
		
		return new MutableObjectIterator<T>() {
			
			private final SingleSegmentInputView in = startIn;
			private final TypeComparator<T> comp = comparator;
			
			private final int numTotal = size();
			private final int numPerSegment = recordsPerSegment;
			
			private int currentTotal = 0;
			private int currentInSegment = 0;
			private int currentSegmentIndex = 0;

			@Override
			public T next(T reuse) {
				if (this.currentTotal < this.numTotal) {
					
					if (this.currentInSegment >= this.numPerSegment) {
						this.currentInSegment = 0;
						this.currentSegmentIndex++;
						this.in.set(sortBuffer.get(this.currentSegmentIndex), 0);
					}
					
					this.currentTotal++;
					this.currentInSegment++;
					
					try {
						return this.comp.readWithKeyDenormalization(reuse, this.in);
					}
					catch (IOException ioe) {
						throw new RuntimeException(ioe);
					}
				}
				else {
					return null;
				}
			}

			@Override
			public T next() {
				if (this.currentTotal < this.numTotal) {

					if (this.currentInSegment >= this.numPerSegment) {
						this.currentInSegment = 0;
						this.currentSegmentIndex++;
						this.in.set(sortBuffer.get(this.currentSegmentIndex), 0);
					}

					this.currentTotal++;
					this.currentInSegment++;

					try {
						return this.comp.readWithKeyDenormalization(serializer.createInstance(), this.in);
					}
					catch (IOException ioe) {
						throw new RuntimeException(ioe);
					}
				}
				else {
					return null;
				}
			}
		};
	}
	
	// ------------------------------------------------------------------------
	//                Writing to a DataOutputView
	// ------------------------------------------------------------------------
	
	/**
	 * Writes the records in this buffer in their logical order to the given output.
	 * 
	 * @param output The output view to write the records to.
	 * @throws IOException Thrown, if an I/O exception occurred writing to the output view.
	 */
	@Override
	public void writeToOutput(final ChannelWriterOutputView output) throws IOException {
		final TypeComparator<T> comparator = this.comparator;
		final TypeSerializer<T> serializer = this.serializer;
		T record = this.recordInstance;
		
		final SingleSegmentInputView inView = this.inView;
		
		final int recordsPerSegment = this.recordsPerSegment;
		int recordsLeft = this.numRecords;
		int currentMemSeg = 0;
		
		while (recordsLeft > 0) {
			final MemorySegment currentIndexSegment = this.sortBuffer.get(currentMemSeg++);
			inView.set(currentIndexSegment, 0);
			
			// check whether we have a full or partially full segment
			if (recordsLeft >= recordsPerSegment) {
				// full segment
				for (int numInMemSeg = 0; numInMemSeg < recordsPerSegment; numInMemSeg++) {
					record = comparator.readWithKeyDenormalization(record, inView);
					serializer.serialize(record, output);
				}
				recordsLeft -= recordsPerSegment;
			} else {
				// partially filled segment
				for (; recordsLeft > 0; recordsLeft--) {
					record = comparator.readWithKeyDenormalization(record, inView);
					serializer.serialize(record, output);
				}
			}
		}
	}
	
	@Override
	public void writeToOutput(ChannelWriterOutputView output, LargeRecordHandler<T> largeRecordsOutput)
			throws IOException
	{
		writeToOutput(output);
	}
	
	/**
	 * Writes a subset of the records in this buffer in their logical order to the given output.
	 * 
	 * @param output The output view to write the records to.
	 * @param start The logical start position of the subset.
	 * @param num The number of elements to write.
	 * @throws IOException Thrown, if an I/O exception occurred writing to the output view.
	 */
	@Override
	public void writeToOutput(final ChannelWriterOutputView output, final int start, int num) throws IOException {
		final TypeComparator<T> comparator = this.comparator;
		final TypeSerializer<T> serializer = this.serializer;
		T record = this.recordInstance;
		
		final SingleSegmentInputView inView = this.inView;
		
		final int recordsPerSegment = this.recordsPerSegment;
		int currentMemSeg = start / recordsPerSegment;
		int offset = (start % recordsPerSegment) * this.recordSize;
		
		while (num > 0) {
			final MemorySegment currentIndexSegment = this.sortBuffer.get(currentMemSeg++);
			inView.set(currentIndexSegment, offset);
			
			// check whether we have a full or partially full segment
			if (num >= recordsPerSegment && offset == 0) {
				// full segment
				for (int numInMemSeg = 0; numInMemSeg < recordsPerSegment; numInMemSeg++) {
					record = comparator.readWithKeyDenormalization(record, inView);
					serializer.serialize(record, output);
				}
				num -= recordsPerSegment;
			} else {
				// partially filled segment
				for (; num > 0 && offset <= this.lastEntryOffset; num--, offset += this.recordSize) {
					record = comparator.readWithKeyDenormalization(record, inView);
					serializer.serialize(record, output);
				}
			}

			offset = 0;
		}
	}
	
	private static final class SingleSegmentOutputView extends AbstractPagedOutputView {
		
		SingleSegmentOutputView(int segmentSize) {
			super(segmentSize, 0);
		}
		
		void set(MemorySegment segment) {
			seekOutput(segment, 0);
		}
		
		@Override
		protected MemorySegment nextSegment(MemorySegment current, int positionInCurrent) throws IOException {
			throw new EOFException();
		}
	}
	
	private static final class SingleSegmentInputView extends AbstractPagedInputView {
		
		private final int limit;
		
		SingleSegmentInputView(int limit) {
			super(0);
			this.limit = limit;
		}
		
		protected void set(MemorySegment segment, int offset) {
			seekInput(segment, offset, this.limit);
		}
		
		@Override
		protected MemorySegment nextSegment(MemorySegment current) throws EOFException {
			throw new EOFException();
		}

		@Override
		protected int getLimitForSegment(MemorySegment segment) {
			return this.limit;
		}
	}

	/* QuickSort's method */
	public void fix(IndexedSortable s, int pN, int pO, int rN, int rO) {
		if (s.compare(pN, pO, rN, rO) > 0) {
			s.swap(pN, pO, rN, rO);
		}
	}

	/**
	 * Deepest recursion before giving up and doing a heapsort.
	 * Returns 2 * ceil(log(n)).
	 */
	public int getMaxDepth(int x) {
		if (x <= 0) {
			throw new IllegalArgumentException("Undefined for " + x);
		}
		return (32 - Integer.numberOfLeadingZeros(x - 1)) << 2;
	}

	/**
	 * Sort the given range of items using quick sort. {@inheritDoc} If the recursion depth falls below
	 * {@link #getMaxDepth}, then switch to {@link HeapSort}.
	 */
	public void sort(final IndexedSortable s, int p, int r) {
		int recordsPerSegment = s.recordsPerSegment();
		int recordSize = s.recordSize();

		int maxOffset = recordSize * (recordsPerSegment - 1);

		int size = s.size();
		int sizeN = size / recordsPerSegment;
		int sizeO = (size % recordsPerSegment) * recordSize;

		sortInternal(s, recordsPerSegment, recordSize, maxOffset, 0, 0, 0, size, sizeN, sizeO, getMaxDepth(r - p));
	}

	public void sort(IndexedSortable s) {
		sort(s, 0, s.size());
	}

	/**
	 * Sort the given range of items using quick sort. If the recursion depth falls below
	 * {@link #getMaxDepth}, then switch to {@link HeapSort}.
	 *
	 * @param s paged sortable
	 * @param recordsPerSegment number of records per memory segment
	 * @param recordSize number of bytes per record
	 * @param maxOffset offset of a last record in a memory segment
	 * @param p index of first record in range
	 * @param pN page number of first record in range
	 * @param pO page offset of first record in range
	 * @param r index of last-plus-one'th record in range
	 * @param rN page number of last-plus-one'th record in range
	 * @param rO page offset of last-plus-one'th record in range
	 * @param depth recursion depth
	 *
	 * @see #sort(IndexedSortable, int, int)
	 */
	public void sortInternal(final IndexedSortable s, int recordsPerSegment, int recordSize, int maxOffset,
							 int p, int pN, int pO, int r, int rN, int rO, int depth) {
		while (true) {
			if (r - p < 13) {
				int i = p+1, iN, iO; if (pO == maxOffset) { iN = pN+1; iO = 0; } else { iN = pN; iO = pO+recordSize; }

				while (i < r) {
					int j = i, jN = iN, jO = iO;
					int jd = j-1, jdN, jdO; if (jO == 0) { jdN = jN-1; jdO = maxOffset; } else { jdN = jN; jdO = jO-recordSize; }

					while (j > p && s.compare(jdN, jdO, jN, jO) > 0) {
						s.swap(jN, jO, jdN, jdO);

						j = jd; jN = jdN; jO = jdO;
						jd--; if (jdO == 0) { jdN--; jdO = maxOffset; } else { jdO -= recordSize; }
					}

					i++; if (iO == maxOffset) { iN++; iO = 0; } else { iO += recordSize; }
				}
				return;
			}
			if (--depth < 0) {
				// give up
				alt.sort(s, p, r);
				return;
			}

			int rdN, rdO; if (rO == 0) { rdN = rN-1; rdO = maxOffset; } else { rdN = rN; rdO = rO-recordSize; }
			int m = (p+r)>>>1, mN = m / recordsPerSegment, mO = (m % recordsPerSegment) * recordSize;

			// select, move pivot into first position
			fix(s, mN, mO, pN, pO);
			fix(s, mN, mO, rdN, rdO);
			fix(s, pN, pO, rdN, rdO);

			// Divide
			int i = p, iN = pN, iO = pO;
			int j = r, jN = rN, jO = rO;
			int ll = p, llN = pN, llO = pO;
			int rr = r, rrN = rN, rrO = rO;
			int cr;
			while (true) {
				i++; if (iO == maxOffset) { iN++; iO = 0; } else { iO += recordSize; }

				while (i < j) {
					if ((cr = s.compare(iN, iO, pN, pO)) > 0) {
						break;
					}

					if (0 == cr) {
						ll++; if (llO == maxOffset) { llN++; llO = 0; } else { llO += recordSize; }

						if (ll != i) {
							s.swap(llN, llO, iN, iO);
						}
					}

					i++; if (iO == maxOffset) { iN++; iO = 0; } else { iO += recordSize; }
				}

				j--; if (jO == 0) { jN--; jO = maxOffset; } else { jO -= recordSize; }

				while (j > i) {
					if ((cr = s.compare(pN, pO, jN, jO)) > 0) {
						break;
					}

					if (0 == cr) {
						rr--; if (rrO == 0) { rrN--; rrO = maxOffset; } else { rrO -= recordSize; }

						if (rr != j) {
							s.swap(rrN, rrO, jN, jO);
						}
					}

					j--; if (jO == 0) { jN--; jO = maxOffset; } else { jO -= recordSize; }
				}
				if (i < j) {
					s.swap(iN, iO, jN, jO);
				} else {
					break;
				}
			}
			j = i; jN = iN; jO = iO;
			// swap pivot- and all eq values- into position
			while (ll >= p) {
				i--; if (iO == 0) { iN--; iO = maxOffset; } else { iO -= recordSize; }

				s.swap(llN, llO, iN, iO);

				ll--; if (llO == 0) { llN--; llO = maxOffset; } else { llO -= recordSize; }
			}
			while (rr < r) {
				s.swap(rrN, rrO, jN, jO);

				rr++; if (rrO == maxOffset) { rrN++; rrO = 0; } else { rrO += recordSize; }
				j++; if (jO == maxOffset) { jN++; jO = 0; } else { jO += recordSize; }
			}

			// Conquer
			// Recurse on smaller interval first to keep stack shallow
			assert i != j;
			if (i - p < r - j) {
				sortInternal(s, recordsPerSegment, recordSize, maxOffset, p, pN, pO, i, iN, iO, depth);
				p = j; pN = jN; pO = jO;
			} else {
				sortInternal(s, recordsPerSegment, recordSize, maxOffset, j, jN, jO, r, rN, rO, depth);
				r = i; rN = iN; rO = iO;
			}
		}
	}

	public void sort(){
		this.sort(this);
	}
}
