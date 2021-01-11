package com.fmer.tools.parallelsql;

import com.fmer.tools.parallelsql.bean.ArgLocation;
import com.fmer.tools.parallelsql.bean.CliArgs;
import com.fmer.tools.parallelsql.bean.SqlArg;
import com.fmer.tools.parallelsql.collector.SqlResultCollector;
import com.fmer.tools.parallelsql.jdbc.SqlArgTask;
import com.fmer.tools.parallelsql.printer.Progress;
import com.fmer.tools.parallelsql.utils.CliUtils;
import com.fmer.tools.parallelsql.utils.VerboseLogger;
import com.google.common.collect.Lists;
import org.apache.commons.cli.CommandLine;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 启动类
 * @author fengmeng
 * @date 2021/1/10 0:21
 */
@SpringBootApplication
public class ParallelSqlApplication implements CommandLineRunner {
    @Resource
    private JdbcTemplate jdbcTemplate;

    public static void main(String[] args) {
        CliUtils.initBoot();
        CommandLine commandLine = CliUtils.getCommandLine(args);
        CliArgs cliArgs = CliUtils.getCliArgs(commandLine);
        //注入配置
        System.setProperty("host", cliArgs.getHost());
        System.setProperty("port", String.valueOf(cliArgs.getPort()));
        System.setProperty("database", cliArgs.getDatabase());
        System.setProperty("username", cliArgs.getUsername());
        System.setProperty("password", cliArgs.getPassword());
        if(cliArgs.isVerbose()){
            VerboseLogger.log(Arrays.toString(args));
        }
        SpringApplication.run(ParallelSqlApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        CliArgs cliArgs = CliUtils.getCliArgs(CliUtils.getCommandLine(args));
        Iterator<? extends SqlArg> sqlArgIterator = CliUtils.getArgIterator(cliArgs);

        ExecutorService executor = CliUtils.getThreadPool(cliArgs.getThreadNum());
        Progress progress = new Progress();

        SqlResultCollector sqlResultCollector = CliUtils.getSqlResultCollector(cliArgs, progress);
        List<CompletableFuture<Void>> futures = Lists.newArrayList();
        CompletableFuture<Void> preFuture = null;
        while(sqlArgIterator.hasNext()){
            SqlArg sqlArg = sqlArgIterator.next();
            SqlArgTask sqlArgTask = new SqlArgTask(cliArgs, sqlArg, jdbcTemplate, progress);
            final CompletableFuture<Void> finalPreFuture = preFuture;
            CompletableFuture<Void> future = CompletableFuture.supplyAsync(sqlArgTask, executor).thenApply(sqlResult -> {
                if(!cliArgs.isKeepOrder()){
                    sqlResultCollector.add(sqlResult);
                }else{
                    if(finalPreFuture != null){
                        finalPreFuture.join();
                    }
                    sqlResultCollector.add(sqlResult);
                }
                //打印进度
                if(sqlResult.getSqlArg() != null){
                    ArgLocation argLocation = sqlResult.getSqlArg().getArgLocation();
                    progress.setProgress(argLocation);
                    progress.printProgressIfNeed();
                }
                return null;
            });
            preFuture = future;
            futures.add(future);
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);

        sqlResultCollector.finish();
    }

}
