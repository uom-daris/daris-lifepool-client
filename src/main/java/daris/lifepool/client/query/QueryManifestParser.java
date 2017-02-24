package daris.lifepool.client.query;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.pixelmed.dicom.AttributeTag;
import com.pixelmed.dicom.TagFromName;

/**
 * https://docs.google.com/document/d/1skiNkR8lxx_cW9pCW2OEsZn30wYrkLr8n4LR1OgUmMU/edit
 * 
 * @author wliu5
 *
 */

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
            throw new Exception("Failed to parse manifest file: " + manifestFile + ". No query specification found.");
        }
    }

    static Query parseRow(String row) throws Throwable {

        Query query = new Query();

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
        query.putElement(TagFromName.AccessionNumber, Operator.EQUALS, accessionNumber);

        /*
         * column 2: ImageType
         */
        String remaining = row.substring(iEnd);
        String imageTypeSpecs = null;
        if (remaining.matches("^\\ *\\\".+")) {
            iStart = remaining.indexOf('"');
            matcher = Pattern.compile("\\\"\\ *,").matcher(remaining);
            if (!matcher.find(iStart + 1)) {
                throw new Exception("Failed to parse manifest line: " + row);
            }
            iEnd = matcher.start();
            imageTypeSpecs = remaining.substring(iStart + 1, iEnd);
            remaining = remaining.substring(matcher.end());
        } else if (remaining.matches("^\\ *,.+")) {
            imageTypeSpecs = null;
            remaining = remaining.substring(remaining.indexOf(',') + 1);
        } else {
            throw new Exception("Failed to parse manifest line: " + row);
        }
        query.putElement(parseCell(TagFromName.ImageType, imageTypeSpecs));

        /*
         * column 3: modality
         */
        String modalitySpecs = null;
        if (remaining.matches("^\\ *\\\".+")) {
            iStart = remaining.indexOf('"');
            matcher = Pattern.compile("\\\"\\ *,").matcher(remaining);
            if (!matcher.find(iStart + 1)) {
                throw new Exception("Failed to parse manifest line: " + row);
            }
            iEnd = matcher.start();
            modalitySpecs = remaining.substring(iStart + 1, iEnd);
            remaining = remaining.substring(matcher.end());
        } else if (remaining.matches("^\\ *,.+")) {
            modalitySpecs = null;
            remaining = remaining.substring(remaining.indexOf(',') + 1);
        } else {
            throw new Exception("Failed to parse manifest line: " + row);
        }
        query.putElement(parseCell(TagFromName.Modality, modalitySpecs));

        /*
         * column 4: presentation intent type
         */
        String presentationIntentTypeSpecs = null;
        if (remaining.matches("^\\ *\\\".+")) {
            iStart = remaining.indexOf('"');
            matcher = Pattern.compile("\\\"\\ *,").matcher(remaining);
            if (!matcher.find(iStart + 1)) {
                throw new Exception("Failed to parse manifest line: " + row);
            }
            iEnd = matcher.start();
            presentationIntentTypeSpecs = remaining.substring(iStart + 1, iEnd);
            remaining = remaining.substring(matcher.end());
        } else if (remaining.matches("^\\ *,.+")) {
            presentationIntentTypeSpecs = null;
            remaining = remaining.substring(remaining.indexOf(',') + 1);
        } else {
            throw new Exception("Failed to parse manifest line: " + row);
        }
        query.putElement(parseCell(TagFromName.PresentationIntentType, presentationIntentTypeSpecs));

        /*
         * column 5: manufacturer
         */
        String manufacturerSpecs = null;
        if (remaining.matches("^\\ *\\\".+")) {
            iStart = remaining.indexOf('"');
            matcher = Pattern.compile("\\\"\\ *,").matcher(remaining);
            if (!matcher.find(iStart + 1)) {
                throw new Exception("Failed to parse manifest line: " + row);
            }
            iEnd = matcher.start();
            manufacturerSpecs = remaining.substring(iStart + 1, iEnd);
            remaining = remaining.substring(matcher.end());
        } else if (remaining.matches("^\\ *,.+")) {
            manufacturerSpecs = null;
            remaining = remaining.substring(remaining.indexOf(',') + 1);
        } else {
            throw new Exception("Failed to parse manifest line: " + row);
        }
        query.putElement(parseCell(TagFromName.Manufacturer, manufacturerSpecs));

        /*
         * column 6: institutionName
         */
        String institutionNameSpecs = null;
        if (remaining.matches("^\\ *\\\".+")) {
            iStart = remaining.indexOf('"');
            matcher = Pattern.compile("\\\"\\ *,").matcher(remaining);
            if (!matcher.find(iStart + 1)) {
                throw new Exception("Failed to parse manifest line: " + row);
            }
            iEnd = matcher.start();
            institutionNameSpecs = remaining.substring(iStart + 1, iEnd);
            remaining = remaining.substring(matcher.end());
        } else if (remaining.matches("^\\ *,.+")) {
            institutionNameSpecs = null;
            remaining = remaining.substring(remaining.indexOf(',') + 1);
        } else {
            throw new Exception("Failed to parse manifest line: " + row);
        }
        query.putElement(parseCell(TagFromName.InstitutionName, institutionNameSpecs));

        /*
         * column 7: SeriesDescription
         */
        String seriesDescriptionSpecs = null;
        if (remaining.matches("^\\ *\\\".+")) {
            iStart = remaining.indexOf('"');
            matcher = Pattern.compile("\\\"\\ *,").matcher(remaining);
            if (!matcher.find(iStart + 1)) {
                throw new Exception("Failed to parse manifest line: " + row);
            }
            iEnd = matcher.start();
            seriesDescriptionSpecs = remaining.substring(iStart + 1, iEnd);
            remaining = remaining.substring(matcher.end());
        } else if (remaining.matches("^\\ *,.+")) {
            seriesDescriptionSpecs = null;
            remaining = remaining.substring(remaining.indexOf(',') + 1);
        } else {
            throw new Exception("Failed to parse manifest line: " + row);
        }
        query.putElement(parseCell(TagFromName.SeriesDescription, seriesDescriptionSpecs));

        /*
         * column 8: ManufacturerModelName
         */
        String manufacturerModelNameSpecs = null;
        if (remaining.matches("^\\ *\\\".+")) {
            iStart = remaining.indexOf('"');
            matcher = Pattern.compile("\\\"\\ *,").matcher(remaining);
            if (!matcher.find(iStart + 1)) {
                throw new Exception("Failed to parse manifest line: " + row);
            }
            iEnd = matcher.start();
            manufacturerModelNameSpecs = remaining.substring(iStart + 1, iEnd);
            remaining = remaining.substring(matcher.end());
        } else if (remaining.matches("^\\ *,.+")) {
            manufacturerModelNameSpecs = null;
            remaining = remaining.substring(remaining.indexOf(',') + 1);
        } else {
            throw new Exception("Failed to parse manifest line: " + row);
        }
        query.putElement(parseCell(TagFromName.ManufacturerModelName, manufacturerModelNameSpecs));

        /*
         * column 9: AcquisitionDeviceProcessingDescription
         */
        String acquisitionDeviceProcessingDescriptionSpecs = null;
        if (remaining.matches("^\\ *\\\".+")) {
            iStart = remaining.indexOf('"');
            matcher = Pattern.compile("\\\"\\ *,").matcher(remaining);
            if (!matcher.find(iStart + 1)) {
                throw new Exception("Failed to parse manifest line: " + row);
            }
            iEnd = matcher.start();
            acquisitionDeviceProcessingDescriptionSpecs = remaining.substring(iStart + 1, iEnd);
            remaining = remaining.substring(matcher.end());
        } else if (remaining.matches("^\\ *,.+")) {
            acquisitionDeviceProcessingDescriptionSpecs = null;
            remaining = remaining.substring(remaining.indexOf(',') + 1);
        } else {
            throw new Exception("Failed to parse manifest line: " + row);
        }
        query.putElement(parseCell(TagFromName.AcquisitionDeviceProcessingDescription,
                acquisitionDeviceProcessingDescriptionSpecs));

        /*
         * column 10: ViewPosition
         */
        String viewPositionSpecs = null;
        if (remaining.matches("^\\ *\\\".+")) {
            iStart = remaining.indexOf('"');
            matcher = Pattern.compile("\\\"\\ *,").matcher(remaining);
            if (!matcher.find(iStart + 1)) {
                throw new Exception("Failed to parse manifest line: " + row);
            }
            iEnd = matcher.start();
            viewPositionSpecs = remaining.substring(iStart + 1, iEnd);
            remaining = remaining.substring(matcher.end());
        } else if (remaining.matches("^\\ *,.+")) {
            viewPositionSpecs = null;
            remaining = remaining.substring(remaining.indexOf(',') + 1);
        } else {
            throw new Exception("Failed to parse manifest line: " + row);
        }
        query.putElement(parseCell(TagFromName.ViewPosition, viewPositionSpecs));

        /*
         * column 11: ImageLaterality
         */
        String imageLateralitySpecs = null;
        if (remaining.matches("^\\ *\\\".+")) {
            iStart = remaining.indexOf('"');
            matcher = Pattern.compile("\\\"\\ *,{0,1}").matcher(remaining);
            if (!matcher.find(iStart + 1)) {
                throw new Exception("Failed to parse manifest line: " + row);
            }
            iEnd = matcher.start();
            imageLateralitySpecs = remaining.substring(iStart + 1, iEnd);
            remaining = remaining.substring(matcher.end());
        } else if (remaining.matches("^\\ *,.+")) {
            imageLateralitySpecs = null;
            remaining = remaining.substring(remaining.indexOf(',') + 1);
        } else {
            throw new Exception("Failed to parse manifest line: " + row);
        }
        query.putElement(parseCell(TagFromName.ImageLaterality, imageLateralitySpecs));

        return query;

    }

    static QueryElement parseCell(AttributeTag tag, String specs) throws Throwable {

        if (specs == null || specs.trim().isEmpty()) {
            return null;
        }
        List<Predicate> predicates = new ArrayList<Predicate>();
        Pattern pattern = Pattern.compile("\\ *(={1,2}|!=){0,1}\\ *'[^']+'\\ *");
        Matcher matcher = pattern.matcher(specs);
        int start = 0;
        int prevEnd = 0;
        while (start < specs.length() && matcher.find(start)) {
            String ps = specs.substring(matcher.start(), matcher.end());

            int idx = specs.indexOf("&&", prevEnd);
            if (idx >= 0 && idx <= matcher.start()) {
                ps = "&&" + ps;
            } else {
                idx = specs.indexOf("||", prevEnd);
                if (idx >= 0 && idx <= matcher.start()) {
                    ps = "||" + ps;
                }
            }
            predicates.add(parsePredicate(ps));
            prevEnd = matcher.end();
            start = matcher.end() + 1;
        }
        return new QueryElement(tag, predicates);
    }

    static Predicate parsePredicate(String s) throws Throwable {

        s = s.trim();
        LogicOperator lop = null;
        if (s.startsWith(LogicOperator.OR.symbol())) {
            lop = LogicOperator.OR;
            s = s.substring(2);
        }
        if (s.startsWith(LogicOperator.AND.symbol())) {
            lop = LogicOperator.AND;
            s = s.substring(2);
        }
        Operator op = null;
        String value = null;
        if (s.matches("^\\ *!='.+'\\ *")) {
            op = Operator.NOT_EQUALS;

        } else if (s.matches("^\\ *={1,2}'.+'\\ *")) {
            op = Operator.EQUALS;
        }
        if (op != null) {
            int iStart = s.indexOf("'");
            int iEnd = s.lastIndexOf("'");
            if (iStart >= 0 && iEnd > 0) {
                value = s.substring(iStart + 1, iEnd);
            }
            if (value != null && !value.isEmpty()) {
                return new Predicate(lop, op, value);
            }
        }
        throw new Exception("Failed to parse: " + s);
    }

    public static void main(String[] args) throws Throwable {
        // @formatter:off
//        String row = "0008899SM1001,\"=='DERIVED\\PRIMARY\\\\LEFT'\",\"=='MG'||=='MR'\",,\"=='SIEMENS'\",\"=='Essendon Breastscreen'\",\"=='MAMMOGRAM, Screening'\",\"=='Mammomat Inspiration'\",,\"=='MLO'\",\"=='L'\"";
//        Query query = parseRow(row);
//        System.out.println(query);
        // @formatter:on
    }

}
