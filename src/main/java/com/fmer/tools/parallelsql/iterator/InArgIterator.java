package com.fmer.tools.parallelsql.iterator;

import com.fmer.tools.parallelsql.bean.ArgLocation;
import com.fmer.tools.parallelsql.bean.CliArgs;
import com.fmer.tools.parallelsql.bean.InSqlArg;
import com.fmer.tools.parallelsql.constants.ContentTypeEnum;
import com.fmer.tools.parallelsql.constants.StringConstant;
import com.fmer.tools.parallelsql.utils.CsvUtils;
import com.google.common.collect.Lists;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * in型参数迭代器
 * @author fengmeng
 * @date 2021/1/9 15:53
 */
public class InArgIterator implements Iterator<InSqlArg> {
    private CliArgs cliArgs;
    private ContentTypeEnum contentType;
    private ProgressFileInputStream pfis;
    private LineIterator lineIterator;
    private AtomicLong lineCounter;
    private String[] headers;

    public InArgIterator(CliArgs cliArgs){
        this.cliArgs = cliArgs;
        String fileName = FilenameUtils.getBaseName(cliArgs.getOutFile());
        String ext = FilenameUtils.getExtension(cliArgs.getOutFile());
        if(StringUtils.isNotEmpty(ext)){
            this.contentType = ContentTypeEnum.getByValue(ext.toLowerCase());
        }else{
            this.contentType = ContentTypeEnum.CSV;
        }

        lineCounter = new AtomicLong();
        if(fileName.equalsIgnoreCase(StringConstant.STDIN)){
            this.lineIterator = new LineIterator(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        }else{
            try {
                this.pfis = new ProgressFileInputStream(new File(cliArgs.getInFile()));
                this.lineIterator = new LineIterator(new InputStreamReader(pfis, StandardCharsets.UTF_8));
            } catch (FileNotFoundException e) {
                ExceptionUtils.rethrow(e);
            }
        }
        String header = this.lineIterator.nextLine();
        this.headers = CsvUtils.split(header, CsvUtils.getSep(this.contentType));
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
            String[] datas = CsvUtils.split(line, CsvUtils.getSep(this.contentType));
            if(datas.length != this.headers.length){
                throw new RuntimeException("标题行与数据行列数不一致！header: " + Arrays.toString(headers) + ", datas: " + Arrays.toString(datas));
            }
            batchArgs.add(datas);
            lineCounter.incrementAndGet();
        }
        InSqlArg inSqlArg = new InSqlArg(this.headers, batchArgs);
        if(pfis != null){
            inSqlArg.setArgLocation(new ArgLocation(pfis.getCurrentReadSize(), pfis.getFileSize()));
        }else{
            inSqlArg.setArgLocation(new ArgLocation(lineCounter.get(), -1L));
        }
        return inSqlArg;
    }

    @Getter
    public static class ProgressFileInputStream extends FileInputStream{
        private long fileSize;
        private long currentReadSize;

        public ProgressFileInputStream(File file) throws FileNotFoundException {
            super(file);
            this.fileSize = FileUtils.sizeOf(file);
            this.currentReadSize = 0L;
        }

        @Override
        public int read() throws IOException {
            int c = super.read();
            currentReadSize += 4;
            return c;
        }

        @Override
        public int read(byte[] b) throws IOException {
            int num = super.read(b);
            currentReadSize += num;
            return num;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int num = super.read(b, off, len);
            currentReadSize += num;
            return num;
        }
    }
}
