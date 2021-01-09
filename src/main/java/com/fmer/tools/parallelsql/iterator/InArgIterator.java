package com.fmer.tools.parallelsql.iterator;

import com.fmer.tools.parallelsql.bean.CliArgs;
import com.fmer.tools.parallelsql.bean.InSqlArg;
import com.fmer.tools.parallelsql.constants.StringConstant;
import com.fmer.tools.parallelsql.utils.CsvUtils;
import com.google.common.collect.Lists;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * in型参数迭代器
 * @author fengmeng
 * @date 2021/1/9 15:53
 */
public class InArgIterator implements Iterator<InSqlArg> {
    private CliArgs cliArgs;
    private LineIterator lineIterator;
    private String[] headers;

    public InArgIterator(CliArgs cliArgs){
        this.cliArgs = cliArgs;

        if(cliArgs.getInFile().equalsIgnoreCase(StringConstant.STDIN)){
            this.lineIterator = new LineIterator(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        }else{
            try {
                this.lineIterator = new LineIterator(new InputStreamReader(new FileInputStream(new File(cliArgs.getInFile())), StandardCharsets.UTF_8));
            } catch (FileNotFoundException e) {
                ExceptionUtils.rethrow(e);
            }
        }
        String header = this.lineIterator.nextLine();
        this.headers = CsvUtils.split(header, ",");
    }

    @Override
    public boolean hasNext() {
        return lineIterator.hasNext();
    }

    @Override
    public InSqlArg next() {
        List<String[]> batchArgs = Lists.newArrayListWithCapacity(this.cliArgs.getBatchSize());
        for(int i=0; i < this.cliArgs.getBatchSize() && lineIterator.hasNext(); i++){
            String line = lineIterator.nextLine();
            String[] datas = CsvUtils.split(line, ",");
            if(datas.length != this.headers.length){
                throw new RuntimeException("标题行与数据行列数不一致！header: " + Arrays.toString(headers) + ", datas: " + Arrays.toString(datas));
            }
            batchArgs.add(datas);
        }
        return new InSqlArg(this.headers, batchArgs);
    }
}
