package com.fmer.tools.parallelsql.utils;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * csv工具类
 * @author fengmeng
 * @date 2021/1/9 19:47
 */
public class CsvUtils {
    public static final String DOT = ",";
    public static final Pattern FPAT = Pattern.compile("\\G\\s*([^,\\n\\r]*|\"[^\"]*(?:\"\"[^\"]*)*\")\\s*(?:,|$)");

    public static String[] split(String line, String sep){
        if(!line.contains("\"")){
            return line.split(sep);
        }else{
            List<String> cols = Lists.newArrayList();
            Matcher m = FPAT.matcher(line);
            while(m.find()){
                String col = m.group(1);
                cols.add(csvUnQuote(col));
            }
            return cols.toArray(new String[0]);
        }
    }

    public static String concat(String[] cols, String sep){
        return Arrays.stream(cols).map(s -> CsvUtils.csvQuote(s, sep)).collect(Collectors.joining(sep));
    }

    public static String concat(Collection<String> cols, String sep){
        return cols.stream().map(s -> CsvUtils.csvQuote(s, sep)).collect(Collectors.joining(sep));
    }

    private static String csvQuote(String s, String sep){
        if(s == null){
            return "";
        }
        if(s.contains(sep) || s.contains("\"")){
            s = s.replace("\"", "\"\"");
            return "\"" + s + "\"";
        }
        return s;
    }
    private static String csvUnQuote(String s){
        if(s == null){
            return "";
        }
        if(s.startsWith("\"") && s.endsWith("\"")){
            return s.substring(0, s.length() -1).replace("\"\"", "\"");
        }
        return s;
    }
}
