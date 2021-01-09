package com.fmer.tools.parallelsql.printer;

import com.fmer.tools.parallelsql.bean.CliArgs;
import com.fmer.tools.parallelsql.bean.SqlResult;
import com.fmer.tools.parallelsql.constants.ContentTypeEnum;
import com.fmer.tools.parallelsql.constants.StringConstant;
import com.fmer.tools.parallelsql.jdbc.RowData;
import com.fmer.tools.parallelsql.utils.CsvUtils;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 数据往文件打印
 * @author fengmeng
 * @date 2021/1/9 16:39
 */
public class FileDataPrinter extends DataPrinter {
    private CliArgs cliArgs;
    private AtomicInteger i;
    private OutputStreamWriter outputStreamWriter;
    private volatile Set<String> fieldNameSet;
    private ContentTypeEnum contentType;

    public FileDataPrinter(CliArgs cliArgs){
        this.cliArgs = cliArgs;
        this.i = new AtomicInteger();
        if(cliArgs.getOutFile().equalsIgnoreCase(StringConstant.STDOUT)){
            if(this.cliArgs.isVerbose()){
                System.err.println("outFile: " + cliArgs.getOutFile());
            }
            this.outputStreamWriter = new OutputStreamWriter(System.out, StandardCharsets.UTF_8);
            this.contentType = ContentTypeEnum.JSON;
        }else{
            File file = new File(cliArgs.getOutFile());
            if(this.cliArgs.isVerbose()){
                System.err.println("outFile: " + file.getAbsolutePath());
            }
            String ext = FilenameUtils.getExtension(file.getName());
            if(StringUtils.isNotEmpty(ext)){
                this.contentType = ContentTypeEnum.getByValue(ext.toLowerCase());
            }else{
                this.contentType = ContentTypeEnum.JSON;
            }
            try {
                this.outputStreamWriter = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8);
            } catch (FileNotFoundException e) {
                ExceptionUtils.rethrow(e);
            }
        }

    }

    @Override
    public void print(SqlResult sqlResult){
        List<String> lines = Lists.newArrayList();
        if(i.get() == 0){
            this.fieldNameSet = sqlResult.getTableData().getFieldMap().keySet();
            if(this.contentType.equals(ContentTypeEnum.CSV)){
                List<String> fieldNameList = Lists.newArrayList();
                fieldNameList.add("tags");
                fieldNameList.addAll(this.fieldNameSet);
                lines.add(CsvUtils.concat(fieldNameList, CsvUtils.DOT));
            }else{
                lines.add("[");
            }
        }
        if(sqlResult.getE() != null){
            lines.add(CsvUtils.concat(new String[]{String.valueOf(sqlResult.getSqlArg()), "执行发生异常, e: " + sqlResult.getE().getMessage()}, CsvUtils.DOT));
        }else if(CollectionUtils.isNotEmpty(sqlResult.getTableData().getRows())){
            for(RowData rowData : sqlResult.getTableData().getRows()){
                if(this.contentType.equals(ContentTypeEnum.CSV)){
                    List<String> cols = Lists.newArrayList();
                    cols.add(getTagString(rowData.getTags()));
                    cols.addAll(fieldNameSet.stream().map(k -> Objects.toString(rowData.getColumnDataMap().get(k), null)).collect(Collectors.toList()));
                    lines.add(CsvUtils.concat(cols, CsvUtils.DOT));
                }else{
                    JsonObject json = new JsonObject();
                    json.addProperty("tags", getTagString(rowData.getTags()));
                    json.add("data", new Gson().toJsonTree(rowData.getColumnDataMap()));
                    lines.add(json.toString());
                }
            }
        }

        if(CollectionUtils.isNotEmpty(lines)){
            try {
                IOUtils.writeLines(lines, null, outputStreamWriter);
            } catch (IOException e) {
                ExceptionUtils.rethrow(e);
            }
        }
        i.incrementAndGet();
    }

    private String getTagString(Object[] tags){
        return Arrays.stream(tags).map(s -> Objects.toString(s, "")).collect(Collectors.joining("_"));
    }

    @Override
    public void close(){
        try {
            if(this.contentType.equals(ContentTypeEnum.JSON)){
                IOUtils.writeLines(Collections.singleton("]"), null, outputStreamWriter);
            }
            this.outputStreamWriter.flush();
            this.outputStreamWriter.close();
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }
}
