
package cn.com.yusys;



import cn.com.yusys.SparkTest;

/**
 * @version 1.0.0
 * @项目名称: spark01
 * @类名称: cn.com.yusys
 * @类描述:
 * @功能描述:
 * @创建人: tianfs1@yusys.com.cn
 * @创建时间: 2018/12/26
 * @修改备注:
 * @修改记录: 修改时间    修改人员    修改原因
 * -------------------------------------------------------------
 * @Copyright (c) 2018宇信科技-版权所有
 */

public class Spark {
    public static void main(String[] args) {
        String path = "hdfs://bigdata01:9000/20181225/crrtest/CCR/ADMIN_AUTH_ACCOUNT_ROLE/ALL/part-00000";
        SparkTest.printToSceen(path);
    }
}

