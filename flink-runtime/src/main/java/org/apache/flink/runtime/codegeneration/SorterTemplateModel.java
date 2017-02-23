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

import org.apache.commons.lang.WordUtils;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.runtime.operators.sort.NormalizedKeySorter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class SorterTemplateModel {
	private final static Integer[] POSSIBLE_FIXEDBYTE_OPERRATORS = {8,4,2,1};
	public final static String TEMPLATE_NAME = "sorter.ftlh";

	private final HashMap<Integer,String> byteOperatorMapping;
	private final TypeComparator typeComparator;
	private final ArrayList<Integer> byteOperators;
	private final String sorterName;

	public SorterTemplateModel(TypeComparator typeComparator){
		this.typeComparator = typeComparator;

		this.byteOperators = generatedSequenceFixedByteOperators(typeComparator.getNormalizeKeyLen());

		this.byteOperatorMapping = new HashMap<>();

		this.byteOperatorMapping.put(8, "Long");
		this.byteOperatorMapping.put(4, "Int");
		this.byteOperatorMapping.put(2, "Short");
		this.byteOperatorMapping.put(1, "Byte");

		this.sorterName = generateCodeFilename();

	}

	public String generateCodeFilename() {
		if( byteOperators.size() == 0 ){
			return "FlexibleSizeSorter";
		}

		String name = "";

		for( Integer opt : byteOperators ) {
			name += byteOperatorMapping.get(opt);
		}

		name += "Sorter";

		return name;
	}

	public Map<String,String> getTemplateVariables() {

		Map<String,String> templateVariables = new HashMap();

		templateVariables.put("name", this.sorterName);

		// generate swap function string
		String swapProcedures = generateSwapProcedures();

		templateVariables.put("swapProcedures", swapProcedures);


		return templateVariables;
	}

	public boolean isSortingKeyFixedSize(){
		System.out.println(typeComparator.getNormalizeKeyLen());
		return true;
	}

	private ArrayList<Integer> generatedSequenceFixedByteOperators(int numberBytes){
		ArrayList<Integer> operators = new ArrayList<>();
		if( numberBytes > NormalizedKeySorter.DEFAULT_MAX_NORMALIZED_KEY_LEN ) {
			return operators;
		}

		// also include offset
		numberBytes += NormalizedKeySorter.OFFSET_LEN;

		// greedy checking index
		int i = 0;
		while( numberBytes > 0 ) {
			int bytes = POSSIBLE_FIXEDBYTE_OPERRATORS[i];
			if( bytes <= numberBytes ) {
				operators.add(bytes);
				numberBytes -= bytes;
			} else {
				i++;
			}
		}
		return operators;
	}

	public ArrayList<Integer> getBytesOperators() {
		return byteOperators;
	}

	public String getSorterName (){
		return this.sorterName;
	}

	private String generateSwapProcedures(){
		String swapProcedures = "";

		if( this.byteOperators.size() > 0 ) {
			String temporaryString = "";
			String firstSegmentString = "";
			String secondSegmentString = "";

			int accOffset = 0;
			for( int i = 0; i  < byteOperators.size(); i++ ){
				int numberByte = byteOperators.get(i);
				int varIndex  = i+1;

				String primitiveClass = byteOperatorMapping.get(numberByte);
				String primitiveType  = primitiveClass.toLowerCase();

				String offsetString = "";
				if( i > 0 ) {
					accOffset += byteOperators.get(i-1);
					offsetString = "+" + accOffset;
				}

				temporaryString     += String.format("%s temp%d = segI.get%s(iBufferOffset%s);\n",primitiveType, varIndex, primitiveClass, offsetString );

				firstSegmentString  += String.format("segI.put%s(iBufferOffset%s, segJ.get%s(jBufferOffset%s));\n", primitiveClass, offsetString, primitiveClass, offsetString);

				secondSegmentString += String.format("segJ.put%s(jBufferOffset%s, temp%d);\n", primitiveClass, offsetString, varIndex);

			}

			swapProcedures = temporaryString
				+ "\n" + firstSegmentString
				+ "\n" + secondSegmentString;
		} else {
			swapProcedures = "segI.swapBytes(this.swapBuffer, segJ, iBufferOffset, jBufferOffset, this.indexEntrySize);";
		}

		return swapProcedures;
	}
}
