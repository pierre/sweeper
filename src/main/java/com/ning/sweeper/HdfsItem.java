/*
 * Copyright 2010 Ning, Inc.
 *
 * Ning licenses this file to you under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.ning.sweeper;

import com.google.common.collect.ImmutableList;
import com.ning.sweeper.config.ContentSummaryTypes;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsItem implements Item
{
    private final FileSystem fs;
    private final Path path;
    private final String name;
    private volatile Long totalSize = null;

    private volatile ImmutableList<Item> children;
    private final ContentSummaryTypes contentSummaryType;

    public HdfsItem(FileSystem fs, String path, ContentSummaryTypes contentSummaryType) throws IOException
    {
        this(fs, fs.getFileStatus(new Path(path)), contentSummaryType);
    }

    private HdfsItem(FileSystem fs, FileStatus status, ContentSummaryTypes contentSummaryType) throws IOException
    {
        this.fs = fs;
        this.path = status.getPath();
        this.contentSummaryType = contentSummaryType;

        if (status.isDir()) {
            this.name = "/" + path.getName();
        }
        else {
            this.name = path.getName();
            this.children = ImmutableList.of();
        }
    }

    @Override
    public String getName()
    {
        return name;
    }

    @Override
    public long getTotalSize()
    {
        if (totalSize == null) {
            try {
                switch (contentSummaryType) {
                    case SPACE_USED:
                        totalSize = fs.getContentSummary(path).getSpaceConsumed();
                        break;
                    case NUMBER_OF_FILES:
                        totalSize = fs.getContentSummary(path).getFileCount();
                        break;
                    default:
                        throw new IllegalArgumentException(String.format("Don't know what to look for (%s)", contentSummaryType));
                }

            }
            catch (IOException e) {
                System.err.println("Failed to get size of " + path);
                e.printStackTrace();

                return -1L;
            }
        }

        return totalSize;
    }

    @Override
    public ImmutableList<Item> getChildren()
    {
        if (children == null) {
            ImmutableList.Builder<Item> children = ImmutableList.builder();

            try {
                for (FileStatus status : fs.listStatus(path)) {
                    children.add(new HdfsItem(fs, status, contentSummaryType));
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }

            this.children = children.build();
        }

        return children;
    }

    @Override
    public String toString()
    {
        return name + ":" + totalSize;
    }
}
