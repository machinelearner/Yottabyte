package com.thoughtworks.yottabyte.vehiclerepairdenormalization.parser;

import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.VehicleRepair;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class VehicleRepairReaderTest {
    @Test
    public void shouldGetVehicleRepairRecordsFromFile() throws IOException {
        String testVehicleRepairStream = Thread.currentThread().getContextClassLoader().getResource("test/vehicleRepair/vehicleRepair").getPath();
        VehicleRepairReader vehicleRepairReader = new VehicleRepairReader(new FileInputStream(testVehicleRepairStream));

        List<VehicleRepair> vehicleRepairs = vehicleRepairReader.parse();

        assertEquals(vehicleRepairs.size(), 6);
    }

}