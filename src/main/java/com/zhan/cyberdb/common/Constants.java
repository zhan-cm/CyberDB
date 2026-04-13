package com.zhan.cyberdb.common;

/**
 * CyberDB 全局常量配置
 */
public class Constants {
    // 定义一个页面的大小为 4KB (4096 Bytes)。这是操作系统和磁盘 I/O 最喜欢的对齐大小。
    public static final int PAGE_SIZE = 4096;
    
    // 定义一个无效的 Page ID，用于页面初始化时的占位
    public static final int INVALID_PAGE_ID = -1;
}