package test.udf;

import org.apache.flink.table.functions.ScalarFunction;

import java.util.Locale;

/**
 * 返回输入字符串的大写
 */
public class ToUpper extends ScalarFunction {
    public String eval(String s){
        return s.toUpperCase();
    }

}
