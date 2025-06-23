package trans.view.function;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.*;

/*
* 还需要增加如下几个方法
*1.AS后的第一个BEGIN去掉
*2.执行失败和成功块去掉
*3.双引号去掉
*4.NVARCHAR函数替换
*5.日期TO_CAHR处理
*6.代码块处理成create
*7.临时表前的冒号去掉
*8.另外生成一个文件在output中，存放删除存储过程的语句，以便到时候演示
* */
public class ProcedureBatchParserProc1 {

    public static void main(String[] args) throws IOException {
        String folderPath="";
        if (args.length>0){
            // 1. 设置 SQL 文件的输入目录
            folderPath = args[0];
        }else {
            // 1. 设置 SQL 文件的输入目录
            folderPath = "D:\\002Data\\002RenBao\\002脚本翻译\\005SRC层\\Input";
        }
        Path inputDir = Paths.get(folderPath);
        Path outputDir = inputDir.getParent().resolve("output");
        Path outputAllDir = inputDir.getParent().resolve("outputAll");
        Path dropProcDir = inputDir.getParent().resolve("outputDropProcAll");
        StringBuilder allSqlBuilder = new StringBuilder(); // 👈 新增
        Set<String> allProcTables = new LinkedHashSet<>(); // 保证不重复 + 有序
        StringBuilder dropBuilder = new StringBuilder();

        // 2. 如果输出目录不存在，则创建
        if (!Files.exists(outputDir)) Files.createDirectories(outputDir);
        if (!Files.exists(outputAllDir)) Files.createDirectories(outputAllDir); // 👈 新增
        if (!Files.exists(dropProcDir)) Files.createDirectories(dropProcDir);

        // 3. 遍历 input 目录下的 .sql 文件
        Files.walk(inputDir)
                .filter(path -> path.toString().endsWith(".sql"))
                .forEach(path -> processSqlFile(path, outputDir, allSqlBuilder, allProcTables));

        // === 将 all.sql 写入 outputAll 目录 所有存储过程的create 语句
        Path allSqlFile = outputAllDir.resolve("all.sql");
        Files.writeString(allSqlFile, allSqlBuilder.toString(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING); // 👈 写入文件

        //输出删除存储过程的语句
        Path dropFile = dropProcDir.resolve("dropProc.sql");
        for (String proc : allProcTables) {
            dropBuilder.append("DROP PROCEDURE IF EXISTS ").append(proc).append(";\n");
        }
        Files.writeString(dropFile, dropBuilder.toString(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    private static void processSqlFile(Path inputPath, Path outputDir, StringBuilder allSqlBuilder, Set<String> allProcTables) {
        try {
            String sql = Files.readString(inputPath, StandardCharsets.UTF_8);

            // === 提取信息 ===
            //表名
            String procTable = extractProcedureTable(sql);
            /*if ("SRC.PROC_LI_SRC_RSF_H_ACT_ITEM_INTERFACE_1".equalsIgnoreCase(procTable) ||
                    "SRC.PROC_LI_SRC_RSF_H_ACT_ITEM_INTERFACE".equalsIgnoreCase(procTable) ||
                    "SRC.PROC_LI_SRC_RSF_H_ACT_ITEM_ID_INFO".equalsIgnoreCase(procTable) ||
                    "SRC.PROC_LI_SRC_COR_H_T_CONTRACT_MASTER".equalsIgnoreCase(procTable) ||
                    "SRC.PROC_LI_SRC_COR_H_T_INSURED_FIRST".equalsIgnoreCase(procTable)
            ) {
                System.out.println("Skip: " + procTable);
                return;
            }*/

            // ✅ 收集 procTable 用于生成 dropProc.sql
            if (procTable != null && !procTable.isEmpty()) {
                allProcTables.add(procTable);
            }

            //参数组
            String paramGroup = extractProcedureParams(sql);
            //临时表
            String tempTable = extractTempTableName(sql);
            //--------------------------------------------------------------
            //构建CREATE PROCEDURE,剔除as后begin,删除注释块，删除执行失败日志，删除执行成功日志
            String transformed = transformProcedureSQL(sql);
            //去除双引号和最后一个 end;
            String finalSql = cleanQuotesAndEnsureFinalEnd(transformed);
            //插入成功和失败日志模块
            String completedSql = insertLogBlockBeforeFinalEnd(finalSql);
            //输出文件路径
            String outputFileName = inputPath.getFileName().toString();
            Path outputFile = outputDir.resolve(outputFileName);
            if(tempTable.length()!=0){
                //去掉临时表前的冒号
                String finalCompletedSql = removeColonBeforeTempTable(completedSql, tempTable);
                //识别xxx=select 块,并替换为create table,并把临时表换成 _MID_TMP
                String finalSqlWithCreate = transformTempTableToCreateTable(finalCompletedSql, tempTable, procTable);
                //特殊场景如while、using处理
                String finalSqlWithSyntaxFix = replaceExecAndWhileSyntax(finalSqlWithCreate);
                // 4. 替换日期函数
                finalSqlWithSyntaxFix=convertDateFunctions(finalSqlWithSyntaxFix);
                // 处理数据类型 如：NVARCHAR 转 VARCHAR
                finalSqlWithSyntaxFix = normalizeDataTypes(finalSqlWithSyntaxFix);
                System.out.println("构建CREATE PROCEDURE,剔除as后begin,删除注释块，删除执行失败日志，删除执行成功日志: " + finalSqlWithSyntaxFix);
                // === 输出原始 SQL 到 output 目录 ===
                Files.writeString(outputFile, finalSqlWithSyntaxFix+"\n/", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

                // === 追加到 allSqlBuilder
                allSqlBuilder.append("-- ------------------------------\n");
                allSqlBuilder.append("-- File: ").append(inputPath.getFileName()).append("\n");
                allSqlBuilder.append("-- ------------------------------\n");
                allSqlBuilder.append(finalSqlWithSyntaxFix).append("\n/\n\n");
            }else {
                //特殊场景如while、using处理
                String finalSqlWithSyntaxFix = replaceExecAndWhileSyntax(completedSql);
                // 4. 替换日期函数
                finalSqlWithSyntaxFix=convertDateFunctions(finalSqlWithSyntaxFix);
                // 处理数据类型 如：NVARCHAR 转 VARCHAR
                finalSqlWithSyntaxFix = normalizeDataTypes(finalSqlWithSyntaxFix);
                System.out.println("构建CREATE PROCEDURE,剔除as后begin,删除注释块，删除执行失败日志，删除执行成功日志: " + finalSqlWithSyntaxFix);
                // === 输出原始 SQL 到 output 目录 ===
                Files.writeString(outputFile, finalSqlWithSyntaxFix+"\n/", StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
                // === 追加到 allSqlBuilder
                allSqlBuilder.append("-- ------------------------------\n");
                allSqlBuilder.append("-- File: ").append(inputPath.getFileName()).append("\n");
                allSqlBuilder.append("-- ------------------------------\n");
                allSqlBuilder.append(finalSqlWithSyntaxFix).append("\n/\n\n");
            }


        } catch (IOException e) {
            System.err.println("Failed to process " + inputPath + ": " + e.getMessage());
        }
    }

    //替换 NVARCHAR
    public static String normalizeDataTypes(String sql) {
        if (sql == null || sql.isEmpty()) return sql;
        return sql.replaceAll("(?i)\\bNVARCHAR\\b", "VARCHAR");
    }

    /**
     * 对代码中的日期函数 TO_DATS(...) 转换为 TO_CHAR(..., 'YYYYMMDD')
     */
    public static String convertDateFunctions(String sql) {
        if (sql == null || sql.isEmpty()) return sql;

        // 1. 替换 TO_DATS(...) → TO_CHAR(..., 'YYYYMMDD')
        sql = sql.replaceAll("(?i)TO_DATS\\s*\\(([^\\)]+)\\)", "TO_CHAR($1,'YYYYMMDD')");

        // Step 1. 临时保护所有已存在的 TO_CHAR(...) 表达式，防止重复包裹
        Map<String, String> charPlaceholders = new LinkedHashMap<>();
        Matcher existingToChar = Pattern.compile("(?i)TO_CHAR\\s*\\(([^\\(\\)]*?)\\s*,\\s*'YYYYMMDD'\\s*\\)").matcher(sql);
        int idx = 0;
        while (existingToChar.find()) {
            String original = existingToChar.group();
            String placeholder = "##__TO_CHAR_" + idx++ + "__##";
            charPlaceholders.put(placeholder, original);
            sql = sql.replace(original, placeholder);
        }

        // 日期字段列表
/*        String[] dateFields = {"create_time", "start_time", "end_time", "begin_date", "end_date"};

        for (String field : dateFields) {
            // 替换 xxx.field → TO_CHAR(xxx.field,'YYYYMMDD')，跳过 AS xxx 和已被 TO_CHAR 包裹的
            sql = sql.replaceAll(
                    "(?i)(?<!\\bAS\\s)(\\b\\w+\\.)(" + field + ")\\b",
                    "TO_CHAR($1$2,'YYYYMMDD')"
            );

            // 替换裸字段 → TO_CHAR(field,'YYYYMMDD')，同样跳过 AS 和已包裹
            sql = sql.replaceAll(
                    "(?i)(?<!\\bAS\\s)(?<!\\.)\\b(" + field + ")\\b",
                    "TO_CHAR($1,'YYYYMMDD')"
            );
        }*/

        // Step 2. 恢复 TO_CHAR 保护内容
        for (Map.Entry<String, String> entry : charPlaceholders.entrySet()) {
            sql = sql.replace(entry.getKey(), entry.getValue());
        }

        return sql;
    }

    //识别xxx=select 块,并替换为create table,并把临时表换成_MID_TMP
    public static String transformTempTableToCreateTable(String finalCompletedSql, String tempTable, String procTable) {
        if (finalCompletedSql == null || tempTable == null || procTable == null) {
            return finalCompletedSql;
        }

        String newTableName = procTable + "_MID_TMP";

        // 1. 匹配 tempTable = SELECT ... ;（可能跨多行）
        String pattern = "(?i)\\b" + Pattern.quote(tempTable) + "\\b\\s*=\\s*SELECT.*?;";
        Pattern selectPattern = Pattern.compile(pattern, Pattern.DOTALL);
        Matcher matcher = selectPattern.matcher(finalCompletedSql);

        StringBuilder result = new StringBuilder();
        boolean replaced = false;

        if (matcher.find()) {
            String matchedBlock = matcher.group();
            int equalIndex = matchedBlock.indexOf('=');
            if (equalIndex != -1) {
                String selectSql = matchedBlock.substring(equalIndex + 1).trim();
                if (selectSql.endsWith(";")) {
                    selectSql = selectSql.substring(0, selectSql.length() - 1).trim();
                }

                String replacement = "DROP TABLE IF EXISTS " + newTableName + ";\n"
                        + "CREATE TABLE " + newTableName + " WITH(orientation=column) AS \n" + selectSql + "\n;";

                result.append(finalCompletedSql, 0, matcher.start());
                result.append(replacement);
                result.append(finalCompletedSql.substring(matcher.end()));
                replaced = true;
            }
        }

        String processedSql = replaced ? result.toString() : finalCompletedSql;

        // 2. 替换所有 tempTable（非变量形式）
        processedSql = processedSql.replaceAll("(?i)(?<!:)\\b" + Pattern.quote(tempTable) + "\\b", newTableName);

        // 3. 插入 DROP TABLE 行在“执行成功日志******BEGIN”所在行之前
        String[] lines = processedSql.split("\\r?\\n");
        StringBuilder finalResult = new StringBuilder();
        boolean inserted = false;

        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];

            // 判断是否为“执行成功日志 BEGIN”所在行（宽松匹配）
            if (!inserted && line.toUpperCase().contains("执行成功日志") && line.toUpperCase().contains("BEGIN")) {
                //finalResult.append("--删除中间表\n");
                //finalResult.append("DROP TABLE IF EXISTS ").append(newTableName).append(";\n\n");
                inserted = true;
            }

            finalResult.append(line).append("\n");
        }

        return finalResult.toString();
    }

    public static String replaceExecAndWhileSyntax1(String finalSqlWithCreate) {
        if (finalSqlWithCreate == null || finalSqlWithCreate.isEmpty()) {
            return finalSqlWithCreate;
        }

        String[] lines = finalSqlWithCreate.split("\\r?\\n");
        StringBuilder result = new StringBuilder();

        for (String line : lines) {
            String trimmedLine = line.trim();

            // 1. 替换 exec 为 execute（忽略大小写，只替换以 exec 开头的关键字）
            String updatedLine = line.replaceAll("(?i)\\bexec\\s*:", "execute :");

            // 2. 如果一行中同时包含 :while 和以 do 结尾（忽略大小写），替换 do 为 LOOP
            if (trimmedLine.toLowerCase().contains("while :") &&
                    trimmedLine.toLowerCase().matches(".*\\bdo\\s*$")) {
                updatedLine = updatedLine.replaceAll("(?i)\\bdo\\s*$", "LOOP");
            }
            if (trimmedLine.toLowerCase().contains("while :")) {
                updatedLine = updatedLine.replaceAll("(?i)\\bdo\\s*$", "LOOP");
            }

            // 3. 替换 end while; → end loop;
            updatedLine = updatedLine.replaceAll("(?i)\\bend\\s+while\\s*;", "end loop;");

            // 4. 替换 CALL xxx.SLEEP_SECONDS(...) → PERFORM PG_SLEEP(...)
            updatedLine = updatedLine.replaceAll("(?i)\\bCALL\\s+[\\w\\.]+:SLEEP_SECONDS\\s*\\((.*?)\\)\\s*;",
                    "PERFORM PG_SLEEP($1);");

            result.append(updatedLine).append("\n");
        }

        return result.toString();
    }

    //特殊场景如while、using处理
    public static String replaceExecAndWhileSyntax(String sql) {
        if (sql == null || sql.isEmpty()) return sql;

        String[] lines = sql.split("\\r?\\n");
        StringBuilder result = new StringBuilder();

        Pattern execPattern = Pattern.compile("(?i)\\bexec\\s*:");
        Pattern endWhilePattern = Pattern.compile("(?i)\\bend\\s+while\\s*;");
        Pattern callSleepPattern = Pattern.compile("(?i)CALL\\s+(\\S+):SLEEP_SECONDS\\((.*?)\\);");
        Pattern usingSyncPattern = Pattern.compile("(?i)^\\s*USING\\s+SQLSCRIPT_SYNC\\s+AS\\s+SYNCLIB\\s*;\\s*$");

        for (String line : lines) {
            String modified = line;

            // exec : -> execute :
            modified = execPattern.matcher(modified).replaceAll("execute :");

            // while+do -> LOOP
            if (modified.toLowerCase().contains("while :") && modified.trim().toLowerCase().endsWith("do")) {
                modified = modified.replaceAll("(?i)\\bdo\\s*$", "LOOP");
            }

            // end while; -> end loop;
            modified = endWhilePattern.matcher(modified).replaceAll("end loop;");

            // CALL xxx:SLEEP_SECONDS(...) -> PERFORM PG_SLEEP(...)
            Matcher m = callSleepPattern.matcher(modified);
            if (m.find()) {
                String arg = m.group(2).trim();
                modified = "PERFORM PG_SLEEP(" + arg + ");";
            }

            // 注释掉 USING SQLSCRIPT_SYNC AS SYNCLIB;
            if (usingSyncPattern.matcher(modified).find()) {
                modified = "--" + modified.trim();
            }

            result.append(modified).append("\n");
        }

        return result.toString();
    }






    //临时表前的冒号去掉
    public static String removeColonBeforeTempTable(String completedSql, String tempTable) {
        if (completedSql == null || tempTable == null || tempTable.trim().isEmpty()) {
            return completedSql;
        }

        // 构造安全的正则，忽略大小写，匹配 :[空格]*tempTable
        String pattern = "(?i):\\s*" + Pattern.quote(tempTable) + "\\b";
        return completedSql.replaceAll(pattern, tempTable);
    }

    //插入成功和失败日志模块
    public static String insertLogBlockBeforeFinalEnd(String finalSql) {
        if (finalSql == null || finalSql.isEmpty()) return finalSql;

        // 日志内容构建
        StringBuilder logBlock = new StringBuilder();
        logBlock.append("\n/*********执行成功日志******BEGIN*********/\n");
        logBlock.append("    CALL ETL_CTRL.PROC_LI_ETL_LOG(\n");
        logBlock.append("        P_INPUT_JOB_SESSION, V_WF_NAME, V_BELONG_LEVEL,\n");
        logBlock.append("        V_WF_STATUS, P_INPUT_DATADATE, '', ''\n");
        logBlock.append("    );\n");
        logBlock.append("    P_OUT_WF_STATUS := V_WF_STATUS;\n");
        logBlock.append("/*********执行成功日志******END***********/\n\n");

        logBlock.append("/*********执行失败日志******BEGIN*********/\n");
        logBlock.append("EXCEPTION \n");
        logBlock.append("    WHEN OTHERS THEN \n");
        logBlock.append("        V_WF_STATUS := 'E';\n");
        logBlock.append("        P_OUT_WF_STATUS := V_WF_STATUS;\n");
        logBlock.append("        CALL ETL_CTRL.PROC_LI_ETL_LOG(\n");
        logBlock.append("            P_INPUT_JOB_SESSION, V_WF_NAME, V_BELONG_LEVEL,\n");
        logBlock.append("            V_WF_STATUS, P_INPUT_DATADATE, SQLSTATE, LEFT(SQLERRM,2000) \n");
        logBlock.append("        );\n");
        logBlock.append("/*********执行失败日志******END***************/\n");

        // 查找最后一个 end; 的位置（忽略大小写和换行）
        Pattern endPattern = Pattern.compile("(?i)end\\s*;", Pattern.DOTALL);
        Matcher matcher = endPattern.matcher(finalSql);

        int lastMatchStart = -1;
        int lastMatchEnd = -1;

        while (matcher.find()) {
            lastMatchStart = matcher.start();
            lastMatchEnd = matcher.end();
        }

        if (lastMatchStart == -1) {
            // 如果找不到 end;，就直接 append 在最后
            return finalSql.trim() + "\n\n" + logBlock;
        }

        // 插入日志块到最后一个 end; 之前
        String beforeEnd = finalSql.substring(0, lastMatchStart).trim();
        String endStatement = finalSql.substring(lastMatchStart, lastMatchEnd).trim();
        String afterEnd = finalSql.substring(lastMatchEnd).trim();

        return beforeEnd + "\n\n" + logBlock + "\n" + endStatement +
                (afterEnd.isEmpty() ? "" : "\n" + afterEnd);
    }


    // 去除双引号和最后一个 end;
    public static String cleanQuotesAndEnsureFinalEnd(String transformed) {
        if (transformed == null || transformed.isEmpty()) return transformed;

        // 1. 去除所有双引号
        String noQuotes = transformed.replace("\"", "");

        // 2. 删除最后一个 end;，支持大小写、换行和空格
        Pattern endPattern = Pattern.compile("(?i)END\\s*;\\s*$", Pattern.DOTALL);
        Matcher endMatcher = endPattern.matcher(noQuotes);

        String removedEndSql = noQuotes;
        if (endMatcher.find()) {
            removedEndSql = noQuotes.substring(0, endMatcher.start()).trim();
        }

        // 3. 检查是否仍然存在一个非注释的 end;
        // 按行处理，排除注释行（以 -- 开头或包含 /* ... */）
        String[] lines = removedEndSql.split("\\R"); // \R 匹配所有换行符
        boolean hasEnd = false;
        for (int i = lines.length - 1; i >= 0; i--) {
            String line = lines[i].trim();
            if (line.isEmpty() || line.startsWith("--") || line.startsWith("/*")) continue;
            if (line.matches("(?i)^end\\s*;\\s*$")) {
                hasEnd = true;
            }
            break;
        }

        // 4. 如果没有 end; 则补一个
        if (!hasEnd) {
            removedEndSql = removedEndSql + "\nend;";
        }

        return removedEndSql.trim();
    }



    public static String transformProcedureSQL(String sql) {
        try {
            // 1. 提取 PROCEDURE 表名和参数
            Pattern procPattern = Pattern.compile("(?i)CREATE\\s+PROCEDURE\\s+\"([^\"]+)\"\\.\"([^\"]+)\"\\s*\\(");
            Matcher procMatcher = procPattern.matcher(sql);
            if (!procMatcher.find()) return sql;

            String schema = procMatcher.group(1);
            String procedure = procMatcher.group(2);
            int paramStart = procMatcher.end() - 1;

            // 匹配参数部分
            int parenCount = 0, paramEnd = -1;
            for (int i = paramStart; i < sql.length(); i++) {
                if (sql.charAt(i) == '(') parenCount++;
                else if (sql.charAt(i) == ')') {
                    parenCount--;
                    if (parenCount == 0) {
                        paramEnd = i;
                        break;
                    }
                }
            }
            if (paramEnd == -1) return sql;

            String paramGroup = sql.substring(paramStart + 1, paramEnd)
                    .replaceAll("(?i)\\bOUT\\b", "INOUT").trim();

            // 生成新头部
            String newHead = "drop PROCEDURE if exists \"" + schema + "\".\"" + procedure + ";\n"
                    + "create or replace PROCEDURE \"" + schema + "\".\"" + procedure + "\"(" + paramGroup + ")\n AS\n";

            // 替换头部 CREATE PROCEDURE
            sql = sql.substring(0, procMatcher.start()) + newHead + sql.substring(paramEnd + 1);

            // 2. 删除 LANGUAGE SQLSCRIPT ... DEFAULT SCHEMA ... 行
            sql = sql.replaceAll("(?is)LANGUAGE\\s+SQLSCRIPT\\s*\\n\\s*SQL\\s+SECURITY\\s+INVOKER\\s*\\n\\s*DEFAULT\\s+SCHEMA\\s+SRC.*?\\n", "");

            // 3. 删除 AS 到 第一个 BEGIN（非注释内）
            sql = sql.replaceFirst("(?is)\\bAS\\b\\s*\\n\\s*BEGIN", "");

            // 4. 删除执行失败日志注释块
//            sql = removeLogBlock(sql, "执行失败日志");
            sql = removeExceptionBlockAndLog(sql);

            // 5. 删除执行成功日志注释块
            sql = removeLogBlock(sql, "执行成功日志");

            return sql.trim();

        } catch (Exception e) {
            System.err.println("transformProcedureSQL error: " + e.getMessage());
            return sql;
        }
    }

    // 删除以某个关键词命名的注释块，如“执行失败日志”
    private static String removeLogBlock(String sql, String keyword) {
        String[] lines = sql.split("\\r?\\n");
        StringBuilder result = new StringBuilder();
        boolean skip = false;
        boolean matched = false;

        for (String line : lines) {
            String cleanLine = line.replaceAll("\\s+", "");
            if (!skip && cleanLine.contains("/*") && cleanLine.contains(keyword) && cleanLine.toUpperCase().contains("BEGIN")) {
                skip = true;
                matched = true;
                continue;
            }
            if (skip && cleanLine.contains("/*") && cleanLine.contains(keyword) && cleanLine.toUpperCase().contains("END")) {
                skip = false;
                continue;
            }
            if (!skip) {
                result.append(line).append("\n");
            }
        }

        return matched ? result.toString() : sql;
    }

    public static String removeExceptionBlockAndLog(String sql) {
        Pattern handlerPattern = Pattern.compile("(?i)DECLARE\\s+EXIT\\s+HANDLER\\s+FOR\\s+SQLEXCEPTION.*?END\\s*;", Pattern.DOTALL);
        Matcher handlerMatcher = handlerPattern.matcher(sql);

        String result = sql;
        if (handlerMatcher.find()) {
            int start = handlerMatcher.start();
            int end = handlerMatcher.end();
            result = sql.substring(0, start) + sql.substring(end);

            String[] lines = result.split("\\r?\\n");
            StringBuilder builder = new StringBuilder();
            for (String line : lines) {
                String clean = line.replaceAll("\\s+", "");
                if ((clean.contains("执行失败日志") && clean.toUpperCase().contains("BEGIN")) ||
                        (clean.contains("执行失败日志") && clean.toUpperCase().contains("END"))) {
                    continue;
                }
                builder.append(line).append("\n");
            }
            return builder.toString();
        }
        return sql;
    }



    // 提取 CREATE PROCEDURE 后的表名，如 "SRC"."PROC_LI_SRC_ACC_H_T_COMMISSION_FEE"
    private static String extractProcedureTable(String sql) {
        try {
            Pattern p = Pattern.compile("(?i)CREATE\\s+PROCEDURE\\s+\"([^\"]+)\"\\.\"([^\"]+)\"");
            Matcher m = p.matcher(sql);
            if (m.find() && m.groupCount() >= 2) {
                return m.group(1) + "." + m.group(2);
            }
        } catch (Exception e) {
            System.err.println("extractProcedureTable error: " + e.getMessage());
        }
        return "";
    }

    // 提取括号内参数组并将 OUT 替换为 INOUT（注意仅替换单词 OUT）
    private static String extractProcedureParams(String sql) {
        try {
            // 找到 "CREATE PROCEDURE" 的位置
            int startIndex = sql.toUpperCase().indexOf("CREATE PROCEDURE");
            if (startIndex == -1) return "";

            // 从 "CREATE PROCEDURE" 起，查找第一个左括号 (
            int firstParenIndex = sql.indexOf('(', startIndex);
            if (firstParenIndex == -1) return "";

            int parenCount = 0;
            int endParenIndex = -1;

            // 从第一个左括号开始向后扫描，直到括号配对结束
            for (int i = firstParenIndex; i < sql.length(); i++) {
                char c = sql.charAt(i);
                if (c == '(') {
                    parenCount++;
                } else if (c == ')') {
                    parenCount--;
                    if (parenCount == 0) {
                        endParenIndex = i;
                        break;
                    }
                }
            }

            if (endParenIndex != -1) {
                String paramGroup = sql.substring(firstParenIndex + 1, endParenIndex);
                return paramGroup.replaceAll("(?i)\\bOUT\\b", "INOUT").trim();
            }
        } catch (Exception e) {
            System.err.println("extractProcedureParams error: " + e.getMessage());
        }
        return "";
    }


    // 提取临时表名：匹配形如 TEMP_SRC_DATA = SELECT
    private static String extractTempTableName(String sql) {
        try {
            Pattern p = Pattern.compile("\\b(\\w+)\\s*=\\s*SELECT", Pattern.CASE_INSENSITIVE);
            Matcher m = p.matcher(sql);
            if (m.find() && m.groupCount() >= 1) {
                return m.group(1);
            }
        } catch (Exception e) {
            System.err.println("extractTempTableName error: " + e.getMessage());
        }
        return "";
    }
}
