package com.wind.hive;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;


//UDAF是输入多个数据行，产生一个数据行
//用户自定义的UDAF必须是继承了UDAF，且内部包含多个实现了exec的静态类
public class MaxStrUDAF extends UDAF{

    public MaxStrUDAF() {
    }

    public static class MaxiNumberIntUDAFEvaluator implements UDAFEvaluator{

        //最终结果
        private String result;
        //负责初始化计算函数并设置它的内部状态，result是存放最终结果的
        @Override
        public void init() {
            result=null;
        }
        //每次对一个新值进行聚集计算都会调用iterate方法
        public boolean iterate(String value)
        {
            if(value==null)
                return false;
            if(result==null)
                result=value;
            else {
                if(value.length() > result.length())
                result = value;
            }
            return true;
        }

        //Hive需要部分聚集结果的时候会调用该方法
        //会返回一个封装了聚集计算当前状态的对象
        public String terminatePartial()
        {
            return result;
        }
        //合并两个部分聚集值会调用这个方法
        public boolean merge(String other)
        {
            return iterate(other);
        }
        //Hive需要最终聚集结果时候会调用该方法
        public String terminate()
        {
            return result;
        }
    }
}

//注意事项：
//
//        1，用户的UDAF必须继承了org.apache.hadoop.hive.ql.exec.UDAF；
//
//        2，用户的UDAF必须包含至少一个实现了org.apache.hadoop.hive.ql.exec的静态类，诸如常见的实现了 UDAFEvaluator。
//
//        3，一个计算函数必须实