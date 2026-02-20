package base.parse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import base.models.AttributeValue;
import base.models.DataTypes;
import base.models.Record;

public class DMLParser {


    public static void printResultSet(List<String> attributeNames, List<Record> resultSet) {
        if (attributeNames.size() != resultSet.getFirst().attributeList.size()) {
            System.err.println("Name list not equal to record attribute list in size. Exiting...");
            System.exit(1);
        }

        Integer columnWidth = 12; // feel free to play around with this

        List<Integer> columnWidths = new ArrayList<>();
        StringBuilder topLine = new StringBuilder("| ");
        for (String name : attributeNames) {
            StringBuilder col = new StringBuilder();
            if (name.length() < 8) {
                col.append(" ".repeat((8 - name.length()) / 2))
                                .append(name).append(" ".repeat((8 - name.length()) / 2))
                        .append(" | ");
                columnWidths.add(col.length() - 3);
            } else {
                col.append(name).append(" | ");
                columnWidths.add(col.length() - 3);
            }
            topLine.append(col);
        }

        System.out.println("-".repeat(topLine.length() - 1));
        System.out.println(topLine);

        for (Record r : resultSet) {
            StringBuilder line = new StringBuilder("| ");
            System.out.println("-".repeat(topLine.length() - 1));
            for (int i = 0; i < r.attributeList.size(); i++) {
                String dataValue = r.attributeList.get(i).toString();
                if (dataValue.length() > columnWidths.get(i)) {
                    dataValue = dataValue.substring(0, columnWidths.get(i) - 3);
                    line.append(dataValue).append("...").append(" | ");
                } else {
                    line.append(dataValue).append(" ".repeat((columnWidths.get(i)) - dataValue.length() ))
                            .append(" | ");
                }
            }
            System.out.println(line);
        }
        System.out.println("-".repeat(topLine.length() - 1));
        System.out.println("Rows Returned: " + resultSet.size());
    }


}

class testDMLParsing {
    public static void main(String[] args) {
        ArrayList<String> testNames = new ArrayList<>();
        testNames.add("cat");
        testNames.add("parrot");
        testNames.add("alexander");
        Record r1 = new Record(List.of(new AttributeValue<String>("Cat", DataTypes.VARCHAR),
                new AttributeValue<Integer>(18592, DataTypes.INTEGER),
                new AttributeValue<Double>(3.03, DataTypes.DOUBLE)));
        Record r2 = new Record(List.of(new AttributeValue<String>("Catmeister", DataTypes.VARCHAR),
                new AttributeValue<Integer>(192, DataTypes.INTEGER),
                new AttributeValue<Double>(3.13, DataTypes.DOUBLE)));
        Record r3 = new Record(List.of(new AttributeValue<String>("C", DataTypes.VARCHAR),
                new AttributeValue<Integer>(18592342, DataTypes.INTEGER),
                new AttributeValue<Double>(3.03, DataTypes.DOUBLE)));
        Record r4 = new Record(List.of(new AttributeValue<String>("Catmeister", DataTypes.VARCHAR),
                new AttributeValue<Integer>(192, DataTypes.INTEGER),
                new AttributeValue<Double>(3.53, DataTypes.DOUBLE)));


        DMLParser.printResultSet(testNames, List.of(r1, r2, r3, r4));

    }
}
