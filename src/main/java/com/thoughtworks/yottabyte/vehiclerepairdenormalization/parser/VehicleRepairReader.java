package com.thoughtworks.yottabyte.vehiclerepairdenormalization.parser;

import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.VehicleRepair;
import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class VehicleRepairReader {
    private FileInputStream fileInputStream;

    public VehicleRepairReader(FileInputStream fileInputStream) {
        this.fileInputStream = fileInputStream;
    }

    public List<VehicleRepair> parse() throws IOException {
        List<String> vehicleRepairRows = IOUtils.readLines(fileInputStream);
        ArrayList<VehicleRepair> vehicleRepairs = new ArrayList<>();
        for (String vehicleRepairRow : vehicleRepairRows) {
            vehicleRepairs.add(new VehicleRepair(vehicleRepairRow));
        }
        return vehicleRepairs;
    }
}
