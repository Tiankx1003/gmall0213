package com.atguigu.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @Author lzc
 * @Date 2020/6/30 9:19
 */
@Description(name = "json_array_to_struct_array", value = "-- convert json_array to struct_array")
public class JsonArrayToStructArray extends GenericUDF {
    /*
        对输入检测
        返回输出的值的对象检测器
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 1. 对输入检测
        if (arguments.length < 3) {
            throw new UDFArgumentException("参数个数必须执行3个");
        }
        for (ObjectInspector argument : arguments) {
            if (!"string".equals(argument.getTypeName())) {
                throw new UDFArgumentException("参数必须是 string");
            }
        }
        // 2. 返回输出的值的对象检测器
        // array(struct(k:v, k:v), struct(...))
        List<String> fieldNames = new ArrayList<>();  // 结构体的每个k的名字
        List<ObjectInspector> oiList = new ArrayList<>();  // 结构体的每个k的名字

        int size = arguments.length;
        /*for (int i = 1; i < (size - 1) / 2 + 1; i++) {
            String fieldName = getConstantStringValue(arguments, i).split(":")[0];
            fieldNames.add(fieldName);
        }*/

        for (int i = (size - 1) / 2 + 1; i < size; i++) {

            String fieldName = getConstantStringValue(arguments, i).split(":")[0];
            fieldNames.add(fieldName);

            // 不同的类型, 使用不同的检测器
            String type = getConstantStringValue(arguments, i).split(":")[1];
            switch (type) {
                case "string":
                    oiList.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
                    break;
                case "int":
                    oiList.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
                    break;
                case "bigint":
                    oiList.add(PrimitiveObjectInspectorFactory.javaLongObjectInspector);
                    break;

                default:
                    throw new UDFArgumentException("未知的不支持的类型....");
            }
        }

        return ObjectInspectorFactory
                .getStandardListObjectInspector(ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, oiList));
    }

    /*
       对传入的数据做计算, 返回函数最终的值
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        // json_array_to_struct_array(
        //               get_json_object(line,'$.actions'),
        //              'action_id',
        //              'item',
        //              'item_type',
        //              'ts',
        //              'action_id:string',
        //              'item:string',
        //              'item_type:string',
        //              'ts:bigint')
        // array(struct(..), struct(....))

        if (arguments[0].get() == null) {
            return null;
        }

        // 1.获取传入的json数组
        String jsonArrayString = arguments[0].get().toString();
        JSONArray jsonArray = new JSONArray(jsonArrayString);

        // 2. 解析数组中的数据
        // 2.1 最终的数组

        List<List<Object>> result = new ArrayList<>();
        // 2.2 解析出来需要的每个结构体
        for(int i = 0; i < jsonArray.length(); i++){
            List<Object> struct = new ArrayList<>();
            result.add(struct);

            JSONObject obj = jsonArray.getJSONObject(i);
            Iterator<String> keys = obj.keys(); // json对象中的所有的key
            while (keys.hasNext()) {
                /*
                {
                    "displayType":"promotion",
                    "item":"3",
                    "item_type":"sku_id",
                    "order":1
        }
                 */
                String key = keys.next();
                struct.add(obj.get(key));
            }

        }

        return result;
    }

    /*
    select  a(...)
    返回要展示的字符串
     */
    @Override
    public String getDisplayString(String[] children) {
        return getStandardDisplayString("json_array_to_struct_array", children);
    }
}
/*
[
        {
            "displayType":"promotion",
            "item":"3",
            "item_type":"sku_id",
            "order":1
        },
        {
            "displayType":"promotion",
            "item":"1",
            "item_type":"sku_id",
            "order":2
        },
        {
            "displayType":"query",
            "item":"7",
            "item_type":"sku_id",
            "order":3
        },
        {
            "displayType":"promotion",
            "item":"5",
            "item_type":"sku_id",
            "order":4
        }
]
 */