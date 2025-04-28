/*
 * Copyright (C) 2012 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.drift.idl.generator;

import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftUnion;
import com.facebook.drift.annotations.ThriftUnionId;

@ThriftUnion("Combined")
public final class MyUnion
{
    @ThriftField(1)
    public String stringValue;

    @ThriftField(2)
    public Long longValue;

    @ThriftField(3)
    public Fruit fruitValue;

    @ThriftUnionId
    public short id;
}
