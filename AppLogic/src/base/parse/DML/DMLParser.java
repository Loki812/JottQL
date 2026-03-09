package base.parse.DML;

import base.models.AttributeValue;
import base.models.Record;

import java.util.ArrayList;
import java.util.List;

public class DMLParser {


    public static ArrayList<Integer> printTopLine(List<String> attributeNames) {
        ArrayList<Integer> columnWidths = new ArrayList<>();
        StringBuilder topLine = new StringBuilder("| ");

        int maxColumnWidth = 16;
        int minColumnWidth = 8;
        for (String aName : attributeNames) {
            if (aName.length() > maxColumnWidth) {
                topLine.append(aName, 0, 12).append("... ");
                columnWidths.add(maxColumnWidth);
            } else if (aName.length() < minColumnWidth) {
                topLine.append(aName).append(" ".repeat(minColumnWidth - aName.length()));
                columnWidths.add(minColumnWidth);
            } else {
                topLine.append(aName);
                columnWidths.add(aName.length());
            }
            topLine.append(" | ");
        }

        System.out.println("_".repeat(topLine.length() - 1));
        System.out.println(topLine);
        System.out.println("-".repeat(topLine.length() - 1));

        return columnWidths;
    }

    public static void printRecords(List<Integer> columnWidths, ArrayList<Record> records) {

        for (Record r : records) {
            StringBuilder rowLine = new StringBuilder("| ");
            for (int i = 0; i < r.attributeList.size(); i++) {
                String dataValue = r.attributeList.get(i).toString();
                // check if the string representation of the data can fit in cell
                if (dataValue.length() > columnWidths.get(i)) {
                    rowLine.append(dataValue, 0, columnWidths.get(i) - 3).append("...");
                } else {
                    rowLine.append(dataValue).append(" ".repeat(columnWidths.get(i) - dataValue.length()));
                }
                rowLine.append(" | ");
            }
            System.out.println(rowLine);
        }
    }
}

