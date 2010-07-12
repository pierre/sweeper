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

import com.ning.sweeper.config.SweeperConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.skife.config.ConfigurationObjectFactory;

import javax.swing.*;
import java.awt.*;
import java.io.IOException;

public class Sweeper
{
    public static void main(String[] args) throws IOException
    {
        SweeperConfig sweeperConfig = new ConfigurationObjectFactory(System.getProperties()).build(SweeperConfig.class);
        Configuration hadoopConfig = configureHDFSAccess(sweeperConfig);

        drawBrowser(hadoopConfig, sweeperConfig);
    }

    private static Configuration configureHDFSAccess(SweeperConfig config)
    {
        Configuration conf = new Configuration();

        conf.set("fs.default.name", config.getNamenodeUrl());
        conf.set("hadoop.job.ugi", config.getHadoopUgi());

        return conf;
    }

    private static void drawBrowser(Configuration hadoopConfig, SweeperConfig sweeperConfig)
        throws IOException
    {
        FileSystem fs = FileSystem.get(hadoopConfig);
        JFrame frame = new JFrame("Sweeper");

        HdfsItem items = new HdfsItem(fs, sweeperConfig.getPath(), sweeperConfig.getContentSummary());
        SweeperColumns columns = new SweeperColumns(items);

        columns.setBackground(Color.RED);

        frame.setLayout(new BorderLayout());
        frame.setSize(600, 400);
        frame.setLocation(10, 10);
        frame.setContentPane(columns);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);
    }
}
