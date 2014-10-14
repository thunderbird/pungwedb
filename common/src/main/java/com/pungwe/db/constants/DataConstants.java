/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.pungwe.db.constants;

/**
 * Created by ian on 31/07/2014.
 */
public interface DataConstants {
	int MAXIMUM_DOC_SIZE = 20 * 1024 * 1024; // 20MB
	int MINIMUM_DOC_SIZE = 4096; // 1 Block
	int MAX_INDEX_KEY_SIZE = 2048; // 2 keys per Block
}
