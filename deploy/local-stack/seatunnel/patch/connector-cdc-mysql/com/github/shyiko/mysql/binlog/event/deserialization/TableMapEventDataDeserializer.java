/*
 * Copyright 2013 Stanley Shyiko
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog.event.deserialization;

import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventMetadata;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;

/**
 * @author <a href="mailto:stanley.shyiko@gmail.com">Stanley Shyiko</a>
 */
public class TableMapEventDataDeserializer implements EventDataDeserializer<TableMapEventData> {

    private final TableMapEventMetadataDeserializer metadataDeserializer = new TableMapEventMetadataDeserializer();

    @Override
    public TableMapEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        TableMapEventData eventData = new TableMapEventData();
        eventData.setTableId(inputStream.readLong(6));
        // Standard MySQL writes flags = 0x0001 (TM_BIT_LEN_EXACT_F) here and follows it
        // immediately with the payload. ZTE GoldenDB (DN node, V_ALL-DBV6.1.03.12SP1)
        // writes flags = 0x1f01 and inserts 8 extra bytes - two little-endian uint32
        // carrying its group / GTM index - between the post-header and the standard
        // payload, which shifts every downstream field and yields an empty schema and
        // table plus an illegal column type. Detect it by the vendor bits in the high
        // byte of flags and skip those 8 bytes. A standard MySQL server never sets
        // them, so the stock code path is unchanged.
        int flags = inputStream.readInteger(2);
        if ((flags & 0xFF00) == 0x1F00) {
            inputStream.skip(8);
        }
        inputStream.skip(1); // length of database name
        eventData.setDatabase(inputStream.readZeroTerminatedString());
        inputStream.skip(1); // table name
        eventData.setTable(inputStream.readZeroTerminatedString());
        int numberOfColumns = inputStream.readPackedInteger();
        eventData.setColumnTypes(inputStream.read(numberOfColumns));
        inputStream.readPackedInteger(); // metadata length
        eventData.setColumnMetadata(readMetadata(inputStream, eventData.getColumnTypes()));
        eventData.setColumnNullability(inputStream.readBitSet(numberOfColumns, true));
        int metadataLength = inputStream.available();
        TableMapEventMetadata metadata = null;
        if (metadataLength > 0) {
            metadata = metadataDeserializer.deserialize(
                new ByteArrayInputStream(inputStream.read(metadataLength)),
                eventData.getColumnTypes().length,
                numericColumnCount(eventData.getColumnTypes())
            );
        }
        eventData.setEventMetadata(metadata);
        return eventData;
    }

    private int numericColumnCount(byte[] types) {
        int count = 0;
        for (int i = 0; i < types.length; i++) {
            switch (ColumnType.byCode(types[i] & 0xff)) {
                case TINY:
                case SHORT:
                case INT24:
                case LONG:
                case LONGLONG:
                case NEWDECIMAL:
                case FLOAT:
                case DOUBLE:
                    count++;
                    break;
                default:
                    break;
            }
        }
        return count;
    }

    private int[] readMetadata(ByteArrayInputStream inputStream, byte[] columnTypes) throws IOException {
        int[] metadata = new int[columnTypes.length];
        for (int i = 0; i < columnTypes.length; i++) {
            switch(ColumnType.byCode(columnTypes[i] & 0xFF)) {
                case FLOAT:
                case DOUBLE:
                case BLOB:
                case JSON:
                case GEOMETRY:
                    metadata[i] = inputStream.readInteger(1);
                    break;
                case BIT:
                case VARCHAR:
                case NEWDECIMAL:
                    metadata[i] = inputStream.readInteger(2);
                    break;
                case SET:
                case ENUM:
                case STRING:
                    metadata[i] = bigEndianInteger(inputStream.read(2), 0, 2);
                    break;
                case TIME_V2:
                case DATETIME_V2:
                case TIMESTAMP_V2:
                    metadata[i] = inputStream.readInteger(1); // fsp (@see {@link ColumnType})
                    break;
                default:
                    metadata[i] = 0;
            }
        }
        return metadata;
    }

    private static int bigEndianInteger(byte[] bytes, int offset, int length) {
        int result = 0;
        for (int i = offset; i < (offset + length); i++) {
            byte b = bytes[i];
            result = (result << 8) | (b >= 0 ? (int) b : (b + 256));
        }
        return result;
    }

}
