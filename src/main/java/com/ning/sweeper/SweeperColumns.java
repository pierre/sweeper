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

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import java.awt.*;
import java.awt.event.InputEvent;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.event.MouseMotionListener;
import java.awt.event.MouseWheelEvent;
import java.awt.event.MouseWheelListener;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SweeperColumns extends JComponent
{
    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(5);

    private final List<Column> columns = new ArrayList<Column>();
    private final JLabel renderer = new JLabel("<item>");
    private final Font plainFont = renderer.getFont();
    private final Font italicFont = new Font(plainFont.getName(), Font.ITALIC, plainFont.getSize());
    private final ColumnComponent columnComponent = new ColumnComponent();
    private final JScrollBar horizontalScrollBar = new JScrollBar(Adjustable.HORIZONTAL);

    private final int itemHeight;

    private class ColumnComponent extends JComponent
    {
        @Override
        public void setBounds(int x, int y, int width, int height)
        {
            super.setBounds(x, y, width, height);
            updateBounds();
        }

        public void updateBounds()
        {
            int width = getWidth();
            int height = getHeight();
            int offset = 0;
            int scrollOffset = horizontalScrollBar.getValue();

            for (Column column : columns) {
                column.setBounds(offset - scrollOffset, 0, column.width, height);
                offset += column.width;
            }

            horizontalScrollBar.setVisibleAmount(width);
            horizontalScrollBar.setMaximum(offset);

            int scrollValue = horizontalScrollBar.getValue();

            if (scrollValue + width > offset) {
                horizontalScrollBar.setValue(offset - width);
            }

            repaint();
        }
    }

    private class ColumnItem implements Item
    {
        private final Item item;
        private final Future<?> future;
        private volatile long totalSize = -1;
        private volatile boolean selected = false;
        private volatile boolean updating = false;

        public ColumnItem(final Column parent, final Item item)
        {
            this.item = item;
            this.future = EXECUTOR.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    updating = true;

                    try {
                        totalSize = item.getTotalSize();
                    }
                    catch (Exception e) {
                        System.err.println("Failed to fetch " + item);
                        e.printStackTrace();
                        totalSize = -1;
                    }
                    finally {
                        updating = false;
                    }

                    parent.updateItems();
                }
            });
        }

        public String getName()
        {
            return item.getName();
        }

        public long getTotalSize()
        {
            return totalSize;
        }

        public ImmutableList<Item> getChildren()
        {
            return item.getChildren();
        }

        public void cancel()
        {
            future.cancel(true);
        }
    }

    private class Column extends JComponent
    {
        private final JScrollBar verticalScrollBar = new JScrollBar(Adjustable.VERTICAL)
        {
            @Override
            public void repaint()
            {
                Column.this.repaint();
            }
        };
        private final List<ColumnItem> items;
        private volatile List<ColumnItem> sortedItems;

        private volatile int width = 150;
        private volatile ColumnItem selectedItem = null;

        public Column(final List<Item> items)
        {
            ImmutableList.Builder<ColumnItem> itemBuilder = ImmutableList.builder();

            for (Item item : items) {
                itemBuilder.add(new ColumnItem(this, item));
            }

            sortedItems = this.items = itemBuilder.build();
            verticalScrollBar.setUnitIncrement(4);
            verticalScrollBar.setMaximum(items.size() * itemHeight);
            addMouseListener(new MouseListener()
            {
                @Override
                public void mousePressed(MouseEvent e)
                {
                    if (!tryScrollBar(e)) {
                        int y = e.getY() + verticalScrollBar.getValue();
                        int selectedIndex = y / itemHeight;

                        if (selectedIndex < 0 || selectedIndex >= items.size()) {
                            addAfter(Column.this, null);
                        }
                        else {
                            ColumnItem item = sortedItems.get(selectedIndex);

                            if (!item.selected) {
                                if (selectedItem != null) {
                                    selectedItem.selected = false;
                                }

                                selectedItem = item;
                                item.selected = true;
                                addAfter(Column.this, item.getChildren());
                            }
                        }

                        repaint();
                    }
                }

                @Override
                public void mouseClicked(MouseEvent e)
                {
                    tryScrollBar(e);
                }

                @Override
                public void mouseReleased(MouseEvent e)
                {
                    tryScrollBar(e);
                }

                @Override
                public void mouseEntered(MouseEvent e)
                {
                    tryScrollBar(e);
                }

                @Override
                public void mouseExited(MouseEvent e)
                {
                    tryScrollBar(e);
                }
            });
            addMouseMotionListener(new MouseMotionListener()
            {
                @Override
                public void mouseDragged(MouseEvent e)
                {
                    tryScrollBar(e);
                }

                @Override
                public void mouseMoved(MouseEvent e)
                {
                    tryScrollBar(e);
                }
            });
        }

        private boolean tryScrollBar(MouseEvent e)
        {
            int x = e.getX();
            int scrollWidthOffset = width - verticalScrollBar.getPreferredSize().width;

            e.translatePoint(-scrollWidthOffset, 0);
            verticalScrollBar.dispatchEvent(e);
            e.translatePoint(scrollWidthOffset, 0);

            return x > scrollWidthOffset;
        }

        public void paintComponent(Graphics g)
        {
            g = g.create();

            int height = getHeight();
            int scrollWidth = verticalScrollBar.getPreferredSize().width;
            Rectangle bounds = new Rectangle(0, 0, width - scrollWidth, itemHeight);

            g.setColor(Color.WHITE);
            g.fillRect(0, 0, width, height);
            g.setColor(Color.BLACK);

            g.translate(width - scrollWidth, 0);
            verticalScrollBar.setVisibleAmount(height);
            verticalScrollBar.setBounds(0, 0, scrollWidth, height);
            verticalScrollBar.paint(g);
            g.translate(scrollWidth - width, 0);

            int verticalOffset = verticalScrollBar.getValue();
            int startOffset = verticalOffset / itemHeight;
            int heightLeft = height + itemHeight;

            g.translate(0, -(verticalOffset % itemHeight));

            List<ColumnItem> items = sortedItems;

            for (int i = startOffset; i < items.size() && heightLeft > 0; ++i) {
                ColumnItem item = items.get(i);

                renderer.setFont(item.updating ? italicFont : plainFont);
                renderer.setBackground(item.selected ? Color.LIGHT_GRAY : Color.WHITE);

                sizeToLabel(item.getTotalSize(), renderer);
                renderer.setBounds(bounds);
                renderer.setHorizontalAlignment(SwingConstants.RIGHT);
                renderer.paint(g);

                int sizeWidth = renderer.getPreferredSize().width;

                renderer.setForeground(Color.BLACK);
                renderer.setHorizontalAlignment(SwingConstants.LEFT);
                renderer.setSize(bounds.width - sizeWidth, itemHeight);
                renderer.setText(item.getName());
                renderer.paint(g);

                g.translate(0, itemHeight);
                heightLeft -= itemHeight;
            }

            g.dispose();
        }

        private synchronized void updateItems()
        {
            List<ColumnItem> sortedItems = new ArrayList<ColumnItem>(items);

            Collections.sort(sortedItems, new Comparator<ColumnItem>()
            {
                @Override
                public int compare(ColumnItem lhs, ColumnItem rhs)
                {
                    int result = Long.signum(rhs.getTotalSize() - lhs.getTotalSize());

                    if (result == 0) {
                        result = (lhs.updating ? 0 : 1) - (rhs.updating ? 0 : 1);
                    }

                    if (result == 0) {
                        result = lhs.getName().compareTo(rhs.getName());
                    }

                    return result;
                }
            });

            this.sortedItems = sortedItems;
            repaint();
        }

        private void cancel()
        {
            for (ColumnItem item : items) {
                item.cancel();
            }
        }
    }

    public SweeperColumns(Item items)
    {
        super();
        this.itemHeight = renderer.getPreferredSize().height;
        renderer.setOpaque(true);
        renderer.setBorder(new EmptyBorder(0, 2, 0, 2));
        addMouseWheelListener(new MouseWheelListener()
        {
            @Override
            public void mouseWheelMoved(MouseWheelEvent e)
            {
                if (e.getScrollType() == MouseWheelEvent.WHEEL_UNIT_SCROLL) {
                    // Apple uses "shift" key to signal horizontal scrolling:
                    // http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6440198
                    if ((e.getModifiersEx() & InputEvent.SHIFT_DOWN_MASK) != 0) {
                        scroll(horizontalScrollBar, e);
                    }
                    else {
                        Column column = getColumnAt(e.getPoint());

                        if (column != null) {
                            scroll(column.verticalScrollBar, e);
                        }
                    }
                }
            }

            private void scroll(JScrollBar scrollBar, MouseWheelEvent e)
            {
                int scrollAmount = e.getUnitsToScroll() * scrollBar.getUnitIncrement();

                scrollBar.setValue(scrollBar.getValue() + scrollAmount);
                repaint();
            }
        });

        Column root = new Column(Arrays.asList(items));

        this.columns.add(root);
        horizontalScrollBar.setUnitIncrement(4);
        horizontalScrollBar.setBounds(0, 100, 100, 100);
        columnComponent.add(root);
        setLayout(new BorderLayout());
        add(columnComponent, BorderLayout.CENTER);
        add(horizontalScrollBar, BorderLayout.SOUTH);
    }

    private Column getColumnAt(Point point)
    {
        int x = point.x + horizontalScrollBar.getValue();

        for (Column column : columns) {
            if (column.width > x) {
                return column;
            }

            x -= column.width;
        }

        return null;
    }

    private void addAfter(Column parent, List<Item> items)
    {
        Iterator<Column> iterator = columns.iterator();

        while (iterator.hasNext()) {
            Column column = iterator.next();

            if (column == parent) {
                break;
            }
        }

        while (iterator.hasNext()) {
            Column column = iterator.next();

            column.cancel();
            iterator.remove();
            columnComponent.remove(column);
        }

        if (items != null && !items.isEmpty()) {
            List<Item> children = new ArrayList<Item>(items);
            Column child = new Column(children);

            columns.add(child);
            columnComponent.add(child);
        }

        columnComponent.updateBounds();

        int scrollMax = horizontalScrollBar.getMaximum();
        int scrollVisible = horizontalScrollBar.getVisibleAmount();

        if (scrollMax > getWidth()) {
            horizontalScrollBar.setValue(Math.max(0, scrollMax - scrollVisible));
        }
    }

    private static final String[] SIZES = {"KB", "MB", "GB", "TB", "PB"};
    private static final Color[] COLORS = {Color.BLACK, Color.GREEN.darker(), Color.BLUE.darker(), Color.ORANGE.darker(), Color.RED.darker(), Color.PINK.darker()};

    private static void sizeToLabel(double size, JLabel label)
    {
        if (size == -1) {
            label.setText("");
            label.setForeground(Color.BLACK);

            return;
        }

        DecimalFormat format = new DecimalFormat();
        String text;
        int i = -1;

        format.setMaximumFractionDigits(0);

        if (size <= 1023) {
            text = format.format(size);
        }
        else {
            while (size > 1023 && i < SIZES.length - 1) {
                size /= 1024;
                i += 1;
            }

            if (size < 10) {
                format.setMaximumFractionDigits(1);
            }

            text = format.format(size) + " " + SIZES[i];
        }

        label.setText(text);
        label.setForeground(COLORS[i + 1]);
    }
}
