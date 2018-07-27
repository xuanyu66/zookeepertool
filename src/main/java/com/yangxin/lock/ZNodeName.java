package com.yangxin.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author leon on 2018/7/27.
 * @version 1.0
 * @Description: 表示一个ephemeral znode名称，该名称具有序列号并且可以按顺序排序
 */
class ZNodeName implements Comparable<ZNodeName>{
    private final String name;
    private String prefix;
    private int sequence = -1;
    private static final Logger LOGGER = LoggerFactory.getLogger(ZNodeName.class);

    public ZNodeName(String name){
        if (name == null){
            throw new NullPointerException("id cannot be null");
        }
        this.name = name;
        this.prefix = name;
        int idx = name.lastIndexOf('-');
        if (idx >= 0){
            this.prefix = name.substring(0, idx);
            try {
                this.sequence = Integer.parseInt(name.substring(idx + 1));
            }catch (NumberFormatException e){
                LOGGER.error("Number format exception for {}", e);
            }catch (ArrayIndexOutOfBoundsException e){
                LOGGER.error("Array out of bounds for {}", e);
            }
        }
    }

    @Override
    public String toString() {
        return name.toString();
    }

    @Override
    public boolean equals(Object o){
        if (this == o){
            return true;
        }
    }

    @Override
    public int compareTo(ZNodeName o) {
        return 0;
    }
}
