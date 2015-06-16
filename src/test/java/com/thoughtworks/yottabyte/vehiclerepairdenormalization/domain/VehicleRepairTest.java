package com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class VehicleRepairTest {
    @Test
    public void shouldParseARowOfVehicleRepair() {
        VehicleRepair vehicleRepair = new VehicleRepair("car,a193,engine oiling, rupees,1920");

        assertEquals(vehicleRepair.code().toString(), "a193");
        assertEquals(vehicleRepair.vehicleType().toString(), "car");
        assertEquals(vehicleRepair.description().toString(), "engine oiling");
    }

}