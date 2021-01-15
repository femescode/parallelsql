package com.fmer.tools.parallelsql.printer;

import com.fmer.tools.parallelsql.bean.CliArgs;
import com.fmer.tools.parallelsql.bean.SqlResult;
import com.fmer.tools.parallelsql.constants.ContentTypeEnum;
import com.fmer.tools.parallelsql.constants.StringConstant;
import com.fmer.tools.parallelsql.jdbc.RowData;
import com.fmer.tools.parallelsql.utils.CliUtils;
import com.fmer.tools.parallelsql.utils.CsvUtils;
import com.fmer.tools.parallelsql.utils.GsonUtils;
import com.fmer.tools.parallelsql.utils.VerboseLogger;
import com.google.common.collect.Lists;
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
        String fileName = FilenameUtils.getBaseName(cliArgs.getOutFile());
        String ext = FilenameUtils.getExtension(cliArgs.getOutFile());
        if(StringUtils.isNotEmpty(ext)){
            this.contentType = ContentTypeEnum.getByValue(ext.toLowerCase());
        }else{
            this.contentType = ContentTypeEnum.TSV;
        }
        if(fileName.equalsIgnoreCase(StringConstant.STDOUT)){
            if(this.cliArgs.isVerbose()){
                VerboseLogger.log("outFile: " + cliArgs.getOutFile());
            }
            this.outputStreamWriter = new OutputStreamWriter(System.out, StandardCharsets.UTF_8);
        }else{
            File file = new File(cliArgs.getOutFile());
            if(this.cliArgs.isVerbose()){
                VerboseLogger.log("outFile: " + file.getAbsolutePath());
            }
            try {
                this.outputStreamWriter = new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8);
            } catch (FileNotFoundException e) {
                ExceptionUtils.rethrow(e);
            }
        }

    }

    @Override
    public synchronized void print(SqlResult sqlResult){
        List<String> lines = Lists.newArrayList();
        //写入标题
        if(i.get() == 0){
            this.fieldNameSet = sqlResult.getTableData().getFieldMap().keySet();
            if(this.contentType.equals(ContentTypeEnum.CSV) || this.contentType.equals(ContentTypeEnum.TSV)){
                List<String> fieldNameList = Lists.newArrayList();
                fieldNameList.add("tags");
                fieldNameList.addAll(this.fieldNameSet);
                fieldNameList.add("exception");
                lines.add(CsvUtils.concat(fieldNameList, CsvUtils.getSep(this.contentType)));
            }
        }
        if(sqlResult.getE() != null){
            String stacktrace = ExceptionUtils.getStackTrace(sqlResult.getE());
            if(this.contentType.equals(ContentTypeEnum.CSV) || this.contentType.equals(ContentTypeEnum.TSV)){
                List<String> cols = Lists.newArrayList();
                cols.add(getTagString(sqlResult.getSqlArg().getArgs()));
                cols.addAll(fieldNameSet.stream().map(k -> "").collect(Collectors.toList()));
                cols.add(stacktrace);
                lines.add(CsvUtils.concat(cols, CsvUtils.getSep(this.contentType)));
            }else{
                JsonObject json = new JsonObject();
                json.addProperty("tags", getTagString(sqlResult.getSqlArg().getArgs()));
                json.add("data", GsonUtils.getGson().toJsonTree(Collections.emptyMap()));
                json.addProperty("exception", stacktrace);
                lines.add(json.toString());
            }
        }else if(CollectionUtils.isNotEmpty(sqlResult.getTableData().getRows())){
            for(RowData rowData : sqlResult.getTableData().getRows()){
                if(this.contentType.equals(ContentTypeEnum.CSV) || this.contentType.equals(ContentTypeEnum.TSV)){
                    List<String> cols = Lists.newArrayList();
                    cols.add(getTagString(rowData.getTags()));
                    cols.addAll(fieldNameSet.stream().map(k -> CliUtils.getColumnString(rowData.getColumnDataMap().get(k))).collect(Collectors.toList()));
                    cols.add("");
                    lines.add(CsvUtils.concat(cols, CsvUtils.getSep(this.contentType)));
                }else{
                    JsonObject json = new JsonObject();
                    json.addProperty("tags", getTagString(rowData.getTags()));
                    json.add("data", GsonUtils.getGson().toJsonTree(rowData.getColumnDataMap()));
                    json.addProperty("exception", "");
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
    public synchronized void close(){
        try {
            this.outputStreamWriter.flush();
            this.outputStreamWriter.close();
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }
}
