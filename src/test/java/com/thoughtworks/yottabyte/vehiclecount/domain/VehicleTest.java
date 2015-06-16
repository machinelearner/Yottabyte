package com.thoughtworks.yottabyte.vehiclecount.domain;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class VehicleTest {
    @Test
    public void shouldGetVehicleTypeGiveRow() {
        Text truckType = new Vehicle(new Text("TRUCK,3021a8e,ROSS,2007-07-31")).type();
        assertEquals(truckType, new Text("TRUCK"));

        Text carType = new Vehicle(new Text("CAR,dcfaeaf,VIVEK,2013-05-29")).type();
        assertEquals(carType, new Text("CAR"));
    }

    @Test
    public void shouldGetVehicleRegistration() {
        Text truckType = new Vehicle(new Text("TRUCK,3021a8e,ROSS,2007-07-31")).registration();
        assertEquals(truckType, new Text("3021a8e"));

        Text carType = new Vehicle(new Text("CAR,dcfaeaf,VIVEK,2013-05-29")).registration();
        assertEquals(carType, new Text("dcfaeaf"));
    }

}