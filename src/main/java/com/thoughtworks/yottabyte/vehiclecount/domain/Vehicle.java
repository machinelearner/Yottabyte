package com.thoughtworks.yottabyte.vehiclecount.domain;

import com.thoughtworks.yottabyte.vehiclerepairdenormalization.domain.VehicleRepair;
import org.apache.hadoop.io.Text;

public class Vehicle {
    private Text row;
    private String type;
    private String registration;

    public Vehicle(Text row) {

        this.row = row;
        parse();
    }

    private void parse() {
        String[] splits = row.toString().split(",");
        type = splits[0];
        registration = splits[1];
    }

    public Text type() {
        return new Text(type);
    }

    public Text repairItem(VehicleRepair aVehicleRepair) {
        return new Text(String.format("%s,%s,%s,%s", registration(), type(), aVehicleRepair.code(), aVehicleRepair.description()));
    }

    public Text registration() {
        return new Text(registration);
    }
}
