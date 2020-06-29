package com.atguigu.hive.udtf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

/**
 * udtf:  一进多出
 *  1进:   [{}, {}]
 *  多出:  "{}", "{}"
 * @Author lzc
 * @Date 2020/6/29 14:26
 */
@Description(name = "explode_json_array", value = "explode json array ....")
public class ExplodeJsonArray extends GenericUDTF {
    /*
      作用:
      1.检测输入
      2. 返回期望的数据类型的检测器
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 1. 检测输入
        // 1.1 获取到传入的参数
        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();
        if(inputFields.size() != 1)
            throw new UDFArgumentException("函数 explode_json_array 需要正好1个参数");

        ObjectInspector inspector = inputFields.get(0).getFieldObjectInspector();
        if(inspector.getCategory() != ObjectInspector.Category.PRIMITIVE || !inspector.getTypeName().equals("string")){
            throw new UDFArgumentException("函数 explode_json_array 参数类型不匹配, 必须是一个字符串");
        }
        // 2. 返回期望数据类型的检测器
        List<String> names = new ArrayList<>();
        names.add("action");
        List<ObjectInspector> inspectors = new ArrayList<>();
        inspectors.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(names, inspectors);

    }
    /*
        处理数据   [{}, {}, ...]
     */
    @Override
    public void process(Object[] args) throws HiveException {
        String jsonArrayString = args[0].toString();
        JSONArray jsonArray = new JSONArray(jsonArrayString);
        for (int i = 0; i < jsonArray.length(); i++) {
            String col = jsonArray.getString(i);
            String[] cols = new String[1];
            cols[0] = col;
            forward(cols);  // 为什么要传递数组? 有可能炸裂出来多列数据, 所以才需要传递数字
        }
    }
    /**
     * 关闭资源
     * 不用实现
     */
    @Override
    public void close() throws HiveException {

    }
}
