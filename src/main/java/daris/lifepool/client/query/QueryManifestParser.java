package daris.lifepool.client.query;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class QueryManifestParser {

    public static List<Query> parse(File manifestFile) throws Throwable {
        List<Query> queries = new ArrayList<Query>();
        try (Stream<String> stream = Files.lines(manifestFile.toPath())) {
            stream.forEach(row -> {
                // queries.add(parseRow(row));
            });
        }
        if (!queries.isEmpty()) {
            return queries;
        } else {
            return null;
        }

    }

    static void parseRow(String row) throws Throwable {

        /*
         * column 1: AccessionNumber
         */
        Pattern pattern = Pattern.compile("^\\ *[0-9A-Z]+,");
        Matcher matcher = pattern.matcher(row);
        if (!matcher.find()) {
            throw new Exception("Failed to parse row: " + row);
        }
        int iStart = matcher.start();
        int iEnd = matcher.end();
        String accessionNumber = row.substring(0, iEnd - 1).trim();

        /*
         * column 2: 
         */
        String remaining = row.substring(iEnd + 1);
        pattern = Pattern.compile("^\\ *(\\\".+\\\")*,\\ *");
        matcher = pattern.matcher(remaining);
        if (!matcher.find()) {
            throw new Exception("Failed to parse row: " + row);
        }
        iStart = matcher.start();
        iEnd = matcher.end();
        System.out.println(remaining.substring(iStart,iEnd));


    }

    public static void main(String[] args) throws Throwable {
        String row = "0008899SM1001,\"=='DERIVED\\PRIMARY\\\\LEFT'\",\"=='MG'\",,\"=='SIEMENS'\",\"=='Essendon Breastscreen'\",\"=='MAMMOGRAM, Screening'\",\"=='Mammomat Inspiration'\",,\"=='MLO'\",\"=='L'\"";
        parseRow(row);
    }

}
