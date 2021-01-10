package com.fmer.tools.parallelsql.utils;

import com.fmer.tools.parallelsql.bean.CliArgs;
import com.fmer.tools.parallelsql.bean.InSqlArg;
import com.fmer.tools.parallelsql.bean.SqlArg;
import com.fmer.tools.parallelsql.collector.AggCollector;
import com.fmer.tools.parallelsql.collector.QueryCollector;
import com.fmer.tools.parallelsql.collector.SqlResultCollector;
import com.fmer.tools.parallelsql.constants.CollectorEnum;
import com.fmer.tools.parallelsql.constants.DataPrinterEnum;
import com.fmer.tools.parallelsql.constants.SqlTypeEnum;
import com.fmer.tools.parallelsql.iterator.InArgIterator;
import com.fmer.tools.parallelsql.iterator.RangeArgIterator;
import com.fmer.tools.parallelsql.printer.ConsoleDataPrinter;
import com.fmer.tools.parallelsql.printer.DataPrinter;
import com.fmer.tools.parallelsql.printer.FileDataPrinter;
import com.fmer.tools.parallelsql.printer.Progress;
import org.apache.commons.cli.*;
import org.springframework.util.CustomizableThreadCreator;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 命令行工具类
 * @author fengmeng
 * @date 2021/1/9 15:20
 */
public class CliUtils {
    private CliUtils(){}

    public static CommandLine getCommandLine(String... args) {
        DefaultParser parser = new DefaultParser( );
        Options options = new Options( );
        options.addRequiredOption("h", "host", true, "host");
        options.addRequiredOption("P", "port", true, "port");
        options.addRequiredOption("D", "database", true, "database");
        options.addRequiredOption("u", "username", true, "username");
        options.addRequiredOption("p", "password", true, "password");
        options.addRequiredOption("s", "sql", true, "To be execute sql");
        options.addOption("j", "threadNum", true, "threadNum");
        options.addOption("k", "keepOrder", false, "keepOrder");
        options.addOption("", "inFile", true, "in query args File");
        options.addOption("", "batchSize", true, "in query batchSize");
        options.addOption("", "rangeStart", true, "rangeStart");
        options.addOption("", "rangeEnd", true, "rangeEnd");
        options.addOption("", "rangeSpan", true, "rangeSpan");
        options.addOption("r", "reverse", false, "reverse");
        options.addOption("", "collector", true, "collector");
        options.addOption("", "printer", true, "printer");
        options.addOption("o", "outFile", true, "outFile");
        options.addOption("v", "verbose", false, "verbose");
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage() + "\n Usage: \n" +
                    "java -jar target/parallelsql.jar -hlocalhost -P3306 -uroot -pxxxx -Dshop --sql \"select * from order where (order_id,user_id) in (#{in})\" --inFile \"C:\\\\infile.txt\" --batchSize 10 -v -k -r -o temp.csv \n" +
                    "java -jar target/parallelsql.jar -hlocalhost -P3306 -uroot -pxxxx -Dshop --sql \"select * from order where add_time >= #{start} and add_time < #{end} limit 1\" --rangeStart 1610087881 --rangeEnd 1610141407 --rangeSpan 10000 -v -k -r -o temp.json");
            System.exit(1);
        }
        return commandLine;
    }
    public static CliArgs getCliArgs(CommandLine commandLine) {
        //参数解析
        CliArgs cliArgs = new CliArgs();
        cliArgs.setHost(commandLine.getOptionValue("host"));
        cliArgs.setPort(Integer.parseInt(commandLine.getOptionValue("port")));
        cliArgs.setDatabase(commandLine.getOptionValue("database"));
        cliArgs.setUsername(commandLine.getOptionValue("username"));
        cliArgs.setPassword(commandLine.getOptionValue("password"));
        cliArgs.setSql(commandLine.getOptionValue("sql"));
        if(commandLine.hasOption("threadNum")){
            cliArgs.setThreadNum(Integer.parseInt(commandLine.getOptionValue("threadNum")));
        }
        if(commandLine.hasOption("keepOrder")){
            cliArgs.setKeepOrder(true);
        }
        if(commandLine.hasOption("inFile")){
            cliArgs.setInFile(commandLine.getOptionValue("inFile", "-"));
        }
        if(commandLine.hasOption("batchSize")){
            cliArgs.setBatchSize(Integer.parseInt(commandLine.getOptionValue("batchSize")));
        }
        if(commandLine.hasOption("rangeStart")){
            cliArgs.setRangeStart(commandLine.getOptionValue("rangeStart"));
        }
        if(commandLine.hasOption("rangeEnd")){
            cliArgs.setRangeEnd(commandLine.getOptionValue("rangeEnd"));
        }
        if(commandLine.hasOption("rangeSpan")){
            cliArgs.setRangeSpan(commandLine.getOptionValue("rangeSpan"));
        }
        if(commandLine.hasOption("reverse")){
            cliArgs.setReverse(true);
        }
        if(commandLine.hasOption("collector")){
            cliArgs.setCollector(commandLine.getOptionValue("collector"));
        }
        if(commandLine.hasOption("printer")){
            cliArgs.setPrinter(commandLine.getOptionValue("printer"));
        }
        if(commandLine.hasOption("outFile")){
            cliArgs.setOutFile(commandLine.getOptionValue("outFile"));
        }
        if(commandLine.hasOption("verbose")){
            cliArgs.setVerbose(true);
        }
        return cliArgs;
    }

    public static ExecutorService getThreadPool(int threadNum){
        CustomizableThreadCreator threadCreator = new CustomizableThreadCreator();
        threadCreator.setThreadNamePrefix("thread-pool");
        ExecutorService executor = new ThreadPoolExecutor(threadNum, threadNum, 1L, TimeUnit.HOURS, new LinkedBlockingQueue<>(), threadCreator::createThread);
        return executor;
    }

    public static Iterator<? extends SqlArg> getArgIterator(CliArgs cliArgs){
        SqlTypeEnum sqlTypeEnum = CliUtils.getSqlType(cliArgs.getSql());
        Iterator<? extends SqlArg> sqlArgIterator = null;
        if(sqlTypeEnum.equals(SqlTypeEnum.IN)){
            sqlArgIterator = new InArgIterator(cliArgs);
        }else if(sqlTypeEnum.equals(SqlTypeEnum.RANGE)){
            sqlArgIterator = new RangeArgIterator(cliArgs);
        }
        return sqlArgIterator;
    }

    public static SqlResultCollector getSqlResultCollector(CliArgs cliArgs, Progress progress){
        DataPrinter dataPrinter = getDataPrinter(cliArgs);
        CollectorEnum collectorEnum = CollectorEnum.getByValue(cliArgs.getCollector());
        if(collectorEnum.equals(CollectorEnum.QUERY)){
            return new QueryCollector(cliArgs, dataPrinter, progress);
        }else if(collectorEnum.equals(CollectorEnum.AGG)){
            return new AggCollector(cliArgs, dataPrinter, progress);
        }else{
            return new QueryCollector(cliArgs, dataPrinter, progress);
        }
    }

    public static DataPrinter getDataPrinter(CliArgs cliArgs){
        DataPrinterEnum dataPrinterEnum = DataPrinterEnum.getByValue(cliArgs.getPrinter());
        if(dataPrinterEnum.equals(DataPrinterEnum.CONSOLE)){
            return new ConsoleDataPrinter(cliArgs);
        }else if(dataPrinterEnum.equals(DataPrinterEnum.FILE)){
            return new FileDataPrinter(cliArgs);
        }else{
            return new ConsoleDataPrinter(cliArgs);
        }
    }

    public static void initBoot() {
        System.setProperty("file.encoding", "UTF-8");
    }

    /**
     * in模式，sql包含 in (#{in})
     */
    private static final Pattern IN_PATTERN = Pattern.compile("\\s+in\\s+\\((#\\{in})\\)", Pattern.CASE_INSENSITIVE);
    /**
     * 范围模式，sql包含 xxx >= #{start} and xxx < #{end}
     */
    private static final Pattern RANGE_PATTERN = Pattern.compile("([\\w_]+)\\s*>=\\s*(#\\{\\s*start\\s*})\\s+and\\s+\\1\\s*<\\s*(#\\{\\s*end\\s*})", Pattern.CASE_INSENSITIVE);

    /**
     * 获取sql类型
     * @param sql
     * @return
     */
    public static SqlTypeEnum getSqlType(String sql){
        if(IN_PATTERN.matcher(sql).find()){
            return SqlTypeEnum.IN;
        }
        if(RANGE_PATTERN.matcher(sql).find()){
            return SqlTypeEnum.RANGE;
        }
        throw new RuntimeException("无法识别的sql: " + sql);
    }

    public static String getExecutableSql(String sql, SqlArg sqlArg){
        Matcher m = IN_PATTERN.matcher(sql);
        if(m.find()){
            int start = m.start(1);
            int end = m.end(1);
            InSqlArg inSqlArg = (InSqlArg)sqlArg;
            String inSql = inSqlArg.getInSql();
            return sql.substring(0, start) + inSql + sql.substring(end);
        }
        m = RANGE_PATTERN.matcher(sql);
        if(m.find()){
            int start2 = m.start(2);
            int end2 = m.end(2);
            int start3 = m.start(3);
            int end3 = m.end(3);
            return sql.substring(0, start2) + "?" + sql.substring(end2, start3) + "?" + sql.substring(end3);
        }
        throw new RuntimeException("无法识别的sql: " + sql);
    }

}
