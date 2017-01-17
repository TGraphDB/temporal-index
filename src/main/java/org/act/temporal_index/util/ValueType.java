/*
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
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
package org.act.temporal_index.util;

public enum ValueType
{
    DELETION(0x01),
    VALUE(0x00),
    INVALID(0x02);

    public static ValueType getValueTypeByPersistentId(int persistentId)
    {
        persistentId = persistentId & 3;
        switch (persistentId) {
            case 1:
                return DELETION;
            case 0:
                return VALUE;
            case 2:
                return INVALID;
            default:
                throw new IllegalArgumentException("Unknown persistentId " + persistentId);
        }
    }

    private final int persistentId;

    ValueType(int persistentId)
    {
        this.persistentId = persistentId;
    }

    public int getPersistentId()
    {
        return persistentId;
    }
}
